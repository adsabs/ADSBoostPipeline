import os
import json
import pickle
import zlib
import csv
from datetime import datetime, timedelta
import math

import adsboost.models as models
from adsputils import get_date, ADSCelery, u2asc
from contextlib import contextmanager
from sqlalchemy import create_engine, desc, or_
from sqlalchemy.orm import scoped_session, sessionmaker
from adsputils import load_config, setup_logging

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), "../"))
config = load_config(proj_home=proj_home)
logger = setup_logging('app.py', proj_home=proj_home,
                        level=config.get('LOGGING_LEVEL', 'INFO'),
                        attach_stdout=config.get('LOG_STDOUT', True))

class ADSBoostCelery(ADSCelery):
    """
    Celery application for computing boost factors
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = config
    
    def handle_message_payload(self, message=None, payload=None):
        """
        Handles incoming message payload from Master Pipeline
        """
        try:
            parsed_message = json.loads(message)
            logger.info("Processing record from Master Pipeline")
            self.process_boost_request(parsed_message)
                
        except Exception as e:
            logger.error(f"Error handling message payload: {e}")
            raise

    def process_boost_request(self, request):
        """
        Process a single boost request
        
        :param request: Dictionary containing record information
        """
        try:
            # Extract fields from the new message format
            bibcode = request.get('bibcode')
            if not bibcode and 'bib_data' in request:
                bibcode = request['bib_data'].get('bibcode', '')
            
            scix_id = request.get('scix_id')
            if not scix_id and 'bib_data' in request:
                scix_id = request['bib_data'].get('scix_id', '')
            
            if not bibcode:
                logger.error("No bibcode provided in request")
                return
                
            # Compute boost factors
            boost_factors = self.compute_boost_factors(request)
            
            # Store in database
            self.store_boost_factors(bibcode, scix_id, boost_factors)
            
            # Send to master pipeline
            self.send_to_master_pipeline(request, boost_factors)
            
        except Exception as e:
            logger.error(f"Error processing boost request: {e}")
            raise

    def compute_refereed_boost(self, record):
        """
        Compute refereed boost factor for refereed papers
        
        RFC: Increase relevance for refereed papers over other types like abstracts
        Boost factor: 1 if refereed, 0 if not refereed
        
        :param record: Dictionary containing record information
        :return: Float boost factor (1.0 for refereed, 0.0 for non-refereed)
        """
        # Check metrics section first, then bib_data section
        if 'metrics' in record and record['metrics'].get('refereed', False):
            return 1.0
        elif 'bib_data' in record and record['bib_data'].get('refereed', False):
            return 1.0
        return 0.0

    def compute_doctype_boost(self, record):
        """
        Compute document type boost factor using ranking system
        
        Uses DOCTYPE_RANKING from config to convert ranks to scores
        Ranks are mapped to scores evenly spaced between 0 and 1
        
        :param record: Dictionary containing record information
        :return: Float boost factor
        """
        # Check bib_data section for doctype
        doctype = ''
        if 'bib_data' in record:
            doctype = record['bib_data'].get('doctype', '').lower()
        
        if self.config.get("DOCTYPE_RANKING", False):
            doctype_rank = self.config.get("DOCTYPE_RANKING")
            unique_ranks = sorted(set(doctype_rank.values()))
            
            # Map ranks to scores evenly spaced between 0 and 1 (invert: lowest rank gets the highest score)
            rank_to_score = {rank: 1 - (i / (len(unique_ranks) - 1)) for i, rank in enumerate(unique_ranks)}
            
            # Assign scores to each rank
            doctype_scores = {doctype_name: rank_to_score[rank] for doctype_name, rank in doctype_rank.items()}
            
            return doctype_scores.get(doctype, 0.0)  # Default to 0.0 if doctype not found
        
        # Fallback to default if no DOCTYPE_RANKING config
        logger.warning("No DOCTYPE_RANKING found in config, using default boost")
        return 0.0

    def compute_recency_boost(self, record):
        """
        Compute recency boost factor to prevent newer papers from being overshadowed
        
        Implement recency boost with decay function, consider turning off after 24 months
        Options: Reciprocal/inverse function (preferred), exponential decay, linear decay, or sigmoid
        
        :param record: Dictionary containing record information
        :return: Float boost factor
        """
        pub_date = None
        entry_date = None
        
        # Extract dates from bib_data section
        if 'bib_data' in record:
            bib_data = record['bib_data']
            pub_date = bib_data.get('pubdate')
            entry_date = bib_data.get('entry_date')
        
        if not pub_date and not entry_date:
            return 1.0
        
        # Handle pubdate with "00" for day - substitute with "01"
        if pub_date and pub_date.endswith('-00'):
            pub_date = pub_date[:-2] + '01'
        
        # Use earlier of publication date or entry date
        if pub_date and entry_date:
            try:
                pub_datetime = datetime.strptime(pub_date, '%Y-%m-%d')
                entry_datetime = datetime.strptime(entry_date, '%Y-%m-%d')
                reference_date = min(pub_datetime, entry_datetime)
            except:
                return 1.0
        elif pub_date:
            try:
                reference_date = datetime.strptime(pub_date, '%Y-%m-%d')
            except:
                return 1.0
        elif entry_date:
            try:
                reference_date = datetime.strptime(entry_date, '%Y-%m-%d')
            except:
                return 1.0
        else:
            return 1.0
        
        # Calculate age in months
        age_months = (datetime.now() - reference_date).days / 30.44
        
        # Turn off boost after 24 months
        if age_months > 24:
            return 1.0
        
        # Use reciprocal/inverse function (preferred per RFC)
        multiplier = self.config.get('RECENCY_BOOST_MULTIPLIER', 0.1)
        recency_boost = 1.0 / (1.0 + multiplier * age_months)
        
        # Ensure minimum boost
        return recency_boost

    def compute_collection_weights(self, record):
        """
        Compute collection-based weights for a record based on ranking system
        
        :param record: Dictionary containing record information
        :return: Dictionary with collection weights
        """
        # Extract collections from 'classifications' (string or list) or 'bib_data.database'
        # falling back to 'bib_data.database'. Normalize values.
        record_collections = []

        raw_values = None
        if 'classifications' in record and record['classifications']:
            # Handle both string and list formats for classifications
            if isinstance(record['classifications'], str):
                raw_values = [record['classifications']]
            elif isinstance(record['classifications'], list):
                raw_values = record['classifications']
        elif 'bib_data' in record and record['bib_data'].get('database'):
            raw_values = record['bib_data'].get('database')

        if isinstance(raw_values, list):
            record_collections = [str(v).lower().replace(' ', '_') for v in raw_values if v]
        elif isinstance(raw_values, str) and raw_values:
            record_collections = [raw_values.lower().replace(' ', '_')]

        if not record_collections:
            record_collections = ['general']
            is_default_general = True
        
        # Get ranking configuration from config
        collection_rankings = self.config.get('COLLECTION_RANKINGS', {})
        if not collection_rankings:
            logger.warning("No COLLECTION_RANKINGS found in config, using default weights")
            collections = self.config.get('COLLECTIONS', ['astronomy', 'physics', 'earth_science', 'planetary_science', 'heliophysics', 'general'])
            return {f'{collection}_weight': 1.0 for collection in collections}
        
        collections = self.config.get('COLLECTIONS', ['astronomy', 'physics', 'earth_science', 'planetary_science', 'heliophysics', 'general'])
        
        # Find all unique ranks that are actually present in the rankings
        all_ranks = set()
        for rankings in collection_rankings.values():
            for rank in rankings.values():
                if rank is not None:
                    all_ranks.add(rank)
        
        if not all_ranks:
            return {f'{collection}_weight': 1.0 for collection in collections}
        
        # Sort ranks and create rank-to-weight mapping
        # Weights are evenly distributed from 1.0 (highest rank = highest relevance) to 0.1 (lowest rank = lowest relevance)
        # This ensures even the lowest relevance gets a small positive weight
        sorted_ranks = sorted(all_ranks, reverse=True)  # Highest rank first (highest relevance)
        rank_to_weight = {}
        for i, rank in enumerate(sorted_ranks):
            if len(sorted_ranks) == 1:
                # Only one rank, give it weight 1.0
                rank_to_weight[rank] = 1.0
            else:
                # Distribute weights evenly from 1.0 (highest rank) to 0.1 (lowest rank)
                # This ensures even the lowest relevance gets a small positive weight
                weight = 1.0 - (0.9 * i / (len(sorted_ranks) - 1))
                rank_to_weight[rank] = weight
        
        # For each discipline, find the maximum weight across all collections the record belongs to
        collection_weights = {}
        
        # Special case: if record explicitly has 'general' collection, all disciplines get weight 1.0
        if 'general' in record_collections:
            for discipline in collections:
                collection_weights[f'{discipline}_weight'] = 1.0
            return collection_weights

        for discipline in collections:
            # Find the maximum weight for this discipline across all record collections
            max_weight = 0.0
            for record_collection in record_collections:
                collection_table = collection_rankings.get(record_collection, {})
                rank = collection_table.get(discipline)
                if rank is not None:
                    weight = rank_to_weight.get(rank, 0.0)
                    max_weight = max(max_weight, weight)
            
            collection_weights[f'{discipline}_weight'] = max_weight
        
        return collection_weights

    def compute_final_boost(self, boost_factors, record):
        """
        Compute final boost factors for each discipline using the simplified algorithm:
        1. Compute single boost_factor as weighted average of doctype, refereed, and recency
        2. Compute discipline final boosts as discipline_weight * boost_factor
        
        :param boost_factors: Dictionary of individual boost factors
        :param record: Dictionary containing record information
        :return: Dictionary with discipline-specific final boosts
        """
        # Step 4: Compute boost_factor as weighted average of doctype, refereed, and recency
        weights = self.config.get('BOOST_FACTOR_WEIGHTS', {})
        if not weights:
            logger.warning("No BOOST_FACTOR_WEIGHTS found in config, using default weights")
            weights = {
                'refereed_boost': 0.6,
                'doctype_boost': 0.4,
                'recency_boost': 0.0
            }
                
        # Ensure weights sum to 1.0 for proper weighted average
        total_weight = sum(weights.values())
        if total_weight > 0:
            normalized_weights = {k: v/total_weight for k, v in weights.items()}
            boost_factor = (
                boost_factors['refereed_boost'] * normalized_weights['refereed_boost'] +
                boost_factors['doctype_boost'] * normalized_weights['doctype_boost'] +
                boost_factors['recency_boost'] * normalized_weights['recency_boost']
            )
        else:
            # Fallback to simple average if weights are all 0
            boost_factor = sum(boost_factors.values()) / len(boost_factors)
        
        # Step 5: Compute all discipline final boosts as discipline_weight * boost_factor
        collection_weights = self.compute_collection_weights(record)
        collections = self.config.get('COLLECTIONS', ['astronomy', 'physics', 'earth_science', 'planetary_science', 'heliophysics', 'general'])
        
        final_boosts = {}
        for collection in collections:
            final_boosts[f'{collection}_final_boost'] = collection_weights[f'{collection}_weight'] * boost_factor
        
        return final_boosts

    def compute_boost_factors(self, record):
        """
        Compute all boost factors for a record
        
        Compute refereed_boost, doctype_boost, recency_boost, discipline_boosts, 
        and final_boosts
        Combine all boost scores to make discipline-specific "relevance scores"
        
        :param record: Dictionary containing record information
        :return: Dictionary with computed boost factors
        """
        # Extract bibcode and scix_id for logging
        bibcode = record.get('bibcode')
        if not bibcode and 'bib_data' in record:
            bibcode = record['bib_data'].get('bibcode')
        
        scix_id = record.get('scix_id')
        if not scix_id and 'bib_data' in record:
            scix_id = record['bib_data'].get('scix_id')
        
        logger.debug(f"Computing boost factors for record: {bibcode}")
        
        # Compute individual boost factors
        boost_factors = {
            'refereed_boost': self.compute_refereed_boost(record),
            'doctype_boost': self.compute_doctype_boost(record),
            'recency_boost': self.compute_recency_boost(record)
        }
        
        # Compute boost_factor as weighted average of the three basic boosts
        weights = self.config.get('BOOST_FACTOR_WEIGHTS', {})
        
        # Normalize weights
        total_weight = sum(weights.values())
        normalized_weights = {k: v/total_weight for k, v in weights.items()}
        
        boost_factor = (
            boost_factors['refereed_boost'] * normalized_weights['refereed_boost'] +
            boost_factors['doctype_boost'] * normalized_weights['doctype_boost'] +
            boost_factors['recency_boost'] * normalized_weights['recency_boost']
        )
        
        boost_factors['boost_factor'] = boost_factor
        
        # Compute collection weights
        collection_weights = self.compute_collection_weights(record)
        boost_factors.update(collection_weights)
        
        # Compute final boost factors using simplified algorithm
        final_boosts = self.compute_final_boost(boost_factors, record)
        boost_factors.update(final_boosts)
        
        logger.debug(f"Computed boost factors: {boost_factors}")
        return boost_factors

    def store_boost_factors(self, bibcode, scix_id, boost_factors):
        """
        Store boost factors in database
        
        :param bibcode: Bibcode
        :param scix_id: SciX ID
        :param boost_factors: Dictionary of computed boost factors
        """
        try:
            with self.session_scope() as session:
                # Check if record already exists
                existing_record = session.query(models.BoostFactors).filter(
                    or_(
                        and_(models.BoostFactors.bibcode == bibcode, models.BoostFactors.bibcode != None),
                        and_(models.BoostFactors.scix_id == scix_id, models.BoostFactors.scix_id != None)
                    )
                ).first()
                
                if existing_record:
                    # Update existing record
                    existing_record.refereed_boost = boost_factors['refereed_boost']
                    existing_record.doctype_boost = boost_factors['doctype_boost']
                    existing_record.recency_boost = boost_factors['recency_boost']
                    
                    # Update collection weights
                    existing_record.astronomy_weight = boost_factors.get('astronomy_weight')
                    existing_record.physics_weight = boost_factors.get('physics_weight')
                    existing_record.earth_science_weight = boost_factors.get('earth_science_weight')
                    existing_record.planetary_science_weight = boost_factors.get('planetary_science_weight')
                    existing_record.heliophysics_weight = boost_factors.get('heliophysics_weight')
                    existing_record.general_weight = boost_factors.get('general_weight')
                    
                    # Update discipline-specific final boosts
                    existing_record.astronomy_final_boost = boost_factors.get('astronomy_final_boost')
                    existing_record.physics_final_boost = boost_factors.get('physics_final_boost')
                    existing_record.earth_science_final_boost = boost_factors.get('earth_science_final_boost')
                    existing_record.planetary_science_final_boost = boost_factors.get('planetary_science_final_boost')
                    existing_record.heliophysics_final_boost = boost_factors.get('heliophysics_final_boost')
                    existing_record.general_final_boost = boost_factors.get('general_final_boost')
                    
                    logger.debug(f"Updated boost factors for {bibcode or scix_id}")
                else:
                    # Create new record
                    boost_record = models.BoostFactors(
                        bibcode=bibcode,
                        scix_id=scix_id,
                        refereed_boost=boost_factors['refereed_boost'],
                        doctype_boost=boost_factors['doctype_boost'],
                        recency_boost=boost_factors['recency_boost'],
                        
                        # Collection weights
                        astronomy_weight=boost_factors.get('astronomy_weight'),
                        physics_weight=boost_factors.get('physics_weight'),
                        earth_science_weight=boost_factors.get('earth_science_weight'),
                        planetary_science_weight=boost_factors.get('planetary_science_weight'),
                        heliophysics_weight=boost_factors.get('heliophysics_weight'),
                        general_weight=boost_factors.get('general_weight'),
                        
                        # Discipline-specific final boosts
                        astronomy_final_boost=boost_factors.get('astronomy_final_boost'),
                        physics_final_boost=boost_factors.get('physics_final_boost'),
                        earth_science_final_boost=boost_factors.get('earth_science_final_boost'),
                        planetary_science_final_boost=boost_factors.get('planetary_science_final_boost'),
                        heliophysics_final_boost=boost_factors.get('heliophysics_final_boost'),
                        general_final_boost=boost_factors.get('general_final_boost')
                    )
                    session.add(boost_record)
                    logger.debug(f"Created new boost factors for {bibcode or scix_id}")
                
                session.commit()
                
        except Exception as e:
            logger.error(f"Error indexing boost factors: {e}")
            raise

    def send_to_master_pipeline(self, original_record, boost_factors):
        """
        Send computed boost factors back to Master Pipeline
        
        :param original_record: Original record from Master Pipeline
        :param boost_factors: Computed boost factors
        """
        try:
            # Extract bibcode and scix_id from the new message format
            bibcode = original_record.get('bibcode')
            if not bibcode and 'bib_data' in original_record:
                bibcode = original_record['bib_data'].get('bibcode')
            
            scix_id = original_record.get('scix_id')
            if not scix_id and 'bib_data' in original_record:
                scix_id = original_record['bib_data'].get('scix_id')
            
            # Prepare message for Master Pipeline
            message = {
                'bibcode': bibcode,
                'scix_id': scix_id,
                'boosts': {
                    'doctype_boost': boost_factors.get('doctype_boost', 0.0),
                    'refereed_boost': boost_factors.get('refereed_boost', 0.0),
                    'recency_boost': boost_factors.get('recency_boost', 0.0),
                    'boost_factor': boost_factors.get('boost_factor', 0.0),
                    'astronomy_final_boost': boost_factors.get('astronomy_final_boost', 0.0),
                    'physics_final_boost': boost_factors.get('physics_final_boost', 0.0),
                    'earth_science_final_boost': boost_factors.get('earth_science_final_boost', 0.0),
                    'planetary_science_final_boost': boost_factors.get('planetary_science_final_boost', 0.0),
                    'heliophysics_final_boost': boost_factors.get('heliophysics_final_boost', 0.0),
                    'general_final_boost': boost_factors.get('general_final_boost', 0.0)
                },
                'timestamp': get_date().isoformat()
            }
            
            # Send to Master Pipeline
            self.send_message(
                message=json.dumps(message),
                        broker=self.config.get('OUTPUT_CELERY_BROKER'),
        task_name=self.config.get('OUTPUT_TASKNAME')
            )
            
            logger.info(f"Sent boost factors to Master Pipeline for {bibcode}")
            
        except Exception as e:
            logger.error(f"Error sending to Master Pipeline: {e}")
            raise

    def query_boost_factors(self, bibcode=None, scix_id=None):
        """
        Query boost factors from database
        
        :param bibcode: Bibcode to query
        :param scix_id: SciX ID to query
        :return: List of boost factor records
        """
        try:
            with self.session_scope() as session:
                query = session.query(models.BoostFactors)
                
                if bibcode:
                    query = query.filter(models.BoostFactors.bibcode == bibcode)
                elif scix_id:
                    query = query.filter(models.BoostFactors.scix_id == scix_id)
                else:
                    return []
                
                records = query.all()
                return [
                    {
                        'bibcode': record.bibcode,
                        'scix_id': record.scix_id,
                        'refereed_boost': record.refereed_boost,
                        'doctype_boost': record.doctype_boost,
                        'recency_boost': record.recency_boost,
                        
                        # Collection weights
                        'astronomy_weight': record.astronomy_weight,
                        'physics_weight': record.physics_weight,
                        'earth_science_weight': record.earth_science_weight,
                        'planetary_science_weight': record.planetary_science_weight,
                        'heliophysics_weight': record.heliophysics_weight,
                        'general_weight': record.general_weight,
                        
                        # Discipline-specific final boosts
                        'astronomy_final_boost': record.astronomy_final_boost,
                        'physics_final_boost': record.physics_final_boost,
                        'earth_science_final_boost': record.earth_science_final_boost,
                        'planetary_science_final_boost': record.planetary_science_final_boost,
                        'heliophysics_final_boost': record.heliophysics_final_boost,
                        'general_final_boost': record.general_final_boost,
                        
                        'created': record.created.isoformat() if record.created else None
                    }
                    for record in records
                ]
                
        except Exception as e:
            logger.error(f"Error querying boost factors: {e}")
            raise 