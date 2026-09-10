import os
import json
import base64
import pickle
import zlib
import csv
from datetime import datetime, timedelta
import math

import adsboost.models as models
from adsputils import get_date, ADSCelery, u2asc
from contextlib import contextmanager
from sqlalchemy import create_engine, desc, or_, and_
from sqlalchemy.orm import scoped_session, sessionmaker
from adsputils import load_config, setup_logging
from adsmsg import BoostResponseRecord, BoostResponseRecordList
from google.protobuf.json_format import ParseDict, MessageToDict


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
        Handles incoming message payload from Master Pipeline.
        Supports both single BoostRequestRecord and BoostRequestRecordList messages.
        """
        logger.debug(f"Message: {message}")
        logger.debug(f"Payload: {payload}")
        try:
            # Handle protobuf objects, JSON strings, and already parsed dictionaries
            if hasattr(message, 'DESCRIPTOR'):  # This is a protobuf message
                logger.debug("Received protobuf message, converting to dict")
                parsed_message = self._protobuf_to_dict(message)
            elif isinstance(message, str):
                parsed_message = json.loads(message)
            elif isinstance(message, dict):
                parsed_message = message
                logger.debug(f"Parsed message: {parsed_message}")
            else:
                raise ValueError(f"Message must be a protobuf object, string, or dict, got {type(message)}")
            
            # Check if this is a BoostRequestRecordList (has 'boost_requests' key)
            if 'boost_requests' in parsed_message:
                # This is a list message - compute the whole batch, then store and
                # respond once for the batch rather than once per record
                boost_requests = parsed_message.get('boost_requests', [])
                if not isinstance(boost_requests, list):
                    boost_requests = [boost_requests]

                return self.process_boost_request_batch(boost_requests)
            else:
                # This is a single request - process it directly (backward compatibility)
                logger.debug("Processing single boost request from Master Pipeline")
                self.process_boost_request(parsed_message)
                return {'total': 1, 'processed': 1, 'failed': 0, 'retried': 0}

        except Exception as e:
            logger.error(f"Error handling message payload: {e}")
            raise

    def process_boost_request_batch(self, boost_requests):
        """
        Process a list of boost requests as a batch.

        Boost factors are computed per record, but the database write and the
        response to Master Pipeline are both done once for the whole batch. A
        record that fails is skipped so it cannot take the rest of the batch
        down with it.

        :param boost_requests: List of dictionaries containing record information
        :return: Dictionary with total/processed/failed counts
        """
        total = len(boost_requests)
        logger.info("Processing %d boost request(s) from BoostRequestRecordList", total)

        computed = []
        failed = 0
        retry_queue = []

        for idx, request in enumerate(boost_requests):
            try:
                logger.debug("Processing boost request %d/%d", idx + 1, total)
                result = self.compute_boost_request(request)
                if result:
                    computed.append(result)
                else:
                    # No bibcode to key on; retrying cannot help
                    failed += 1
            except Exception as e:
                logger.error(f"Error processing boost request {idx + 1}/{total}: {e}")
                # Continue processing remaining requests even if one fails, and
                # give this one a single retry once the batch is done
                retry_queue.append(request)
                continue

        # Retry the failures once. Most are transient, and a second attempt costs
        # far less than losing the record until the next full run.
        if retry_queue:
            logger.info("Retrying %d failed boost request(s)", len(retry_queue))
            for request in retry_queue:
                try:
                    result = self.compute_boost_request(request)
                    if result:
                        computed.append(result)
                    else:
                        failed += 1
                except Exception as e:
                    bibcode = request.get('bibcode') if isinstance(request, dict) else None
                    logger.error("Boost request failed on retry for %s: %s", bibcode, e)
                    failed += 1

        if computed:
            self.store_boost_factors_batch(computed)
            self.send_batch_to_master_pipeline(computed)

        logger.info("Processed %d/%d boost request(s), %d failed", len(computed), total, failed)
        return {'total': total, 'processed': len(computed), 'failed': failed,
                'retried': len(retry_queue)}

    def compute_boost_request(self, request):
        """
        Parse a single boost request and compute its boost factors.

        Does not touch the database and does not forward anything; the caller
        decides whether to store/send one record at a time or as a batch.

        :param request: Dictionary containing record information
        :return: Dictionary with bibcode, scix_id and boost_factors, or None if
                 the request has no bibcode to key on
        """
        # Parse JSON strings from the message format sent by Master Pipeline
        parsed_request = self._parse_master_pipeline_message(request)

        # Extract fields from the parsed message
        bibcode = parsed_request.get('bibcode')
        scix_id = parsed_request.get('scix_id')

        if not bibcode:
            logger.error("No bibcode provided in request")
            return None

        return {
            'bibcode': bibcode,
            'scix_id': scix_id,
            'boost_factors': self.compute_final_boost(parsed_request)
        }

    def process_boost_request(self, request):
        """
        Process a single boost request

        :param request: Dictionary containing record information
        """
        try:
            result = self.compute_boost_request(request)
            if not result:
                return

            # Store in database
            self.store_boost_factors(result['bibcode'], result['scix_id'],
                                     result['boost_factors'])

            # Send to master pipeline
            self.send_to_master_pipeline(request, result['boost_factors'])

        except Exception as e:
            logger.error(f"Error processing boost request: {e}")
            raise

    def _parse_master_pipeline_message(self, request):
        """
        Parse the message format sent from Master Pipeline
        
        :param request: Raw request from Master Pipeline
        :return: Parsed request with decoded JSON fields
        """
        try:
            parsed = request.copy()
            logger.debug(f"Parsing request with keys: {list(parsed.keys())}")
            
            # Parse bib_data if it's a JSON string
            if 'bib_data' in parsed and isinstance(parsed['bib_data'], str):
                logger.debug(f"Found bib_data string: {parsed['bib_data'][:100]}...")
                try:
                    parsed['bib_data'] = json.loads(parsed['bib_data'])
                    logger.debug(f"Successfully parsed bib_data, now has keys: {list(parsed['bib_data'].keys())}")
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse bib_data JSON: {e}")
                    parsed['bib_data'] = {}
            elif 'bib_data' not in parsed:
                logger.warning("No bib_data found in request")
                parsed['bib_data'] = {}
            else:
                logger.debug(f"bib_data already parsed, type: {type(parsed['bib_data'])}, keys: {list(parsed['bib_data'].keys()) if isinstance(parsed['bib_data'], dict) else 'not a dict'}")
            
            # Parse metrics if it's a JSON string
            if 'metrics' in parsed and isinstance(parsed['metrics'], str):
                try:
                    parsed['metrics'] = json.loads(parsed['metrics'])
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse metrics JSON: {e}")
                    parsed['metrics'] = {}
            elif 'metrics' not in parsed:
                parsed['metrics'] = {}
            
            # Ensure classifications is a list
            if 'classifications' in parsed and not isinstance(parsed['classifications'], list):
                if isinstance(parsed['classifications'], str) and parsed['classifications']:
                    parsed['classifications'] = [parsed['classifications']]
                elif parsed['classifications']:
                    parsed['classifications'] = list(parsed['classifications'])
                else:
                    parsed['classifications'] = []
            elif 'classifications' not in parsed:
                parsed['classifications'] = []
            
            # Ensure collections is a list
            if 'collections' in parsed and not isinstance(parsed['collections'], list):
                if isinstance(parsed['collections'], str) and parsed['collections']:
                    parsed['collections'] = [parsed['collections']]
                elif parsed['collections']:
                    parsed['collections'] = list(parsed['collections'])
                else:
                    parsed['collections'] = []
            elif 'collections' not in parsed:
                parsed['collections'] = []
            
            return parsed
        except Exception as e:
            logger.error(f"Error parsing master pipeline message: {e}")
            raise

    def _decode_record_fields(self, record_dict):
        """
        Decode bib_data and metrics fields in a single record dictionary.
        Handles both base64-encoded bytes fields (from protobuf) and JSON strings.
        
        :param record_dict: Dictionary containing a single boost request record
        :return: Dictionary with decoded bib_data and metrics fields
        """
        def decode_field(field_value, field_name):
            """Helper to decode a field, trying base64 first, then JSON string"""
            if not isinstance(field_value, str):
                return field_value

            # Try base64 decode first (for protobuf bytes fields). validate=True so
            # that a plain JSON string is rejected here instead of being silently
            # stripped of its non-base64 characters and decoded into garbage.
            decoded_str = None
            try:
                decoded_str = base64.b64decode(field_value, validate=True).decode('utf-8')
            except Exception:
                # Not base64, fall back to treating it as a JSON string directly
                decoded_str = field_value

            # Handle case where field might be "None" or other invalid values
            if decoded_str.lower() in ['none', 'null', '']:
                return {}

            try:
                return json.loads(decoded_str)
            except (json.JSONDecodeError, ValueError):
                logger.warning(f"Failed to decode {field_name}: not base64 or valid JSON")
                return {}

        # Decode bib_data if present
        if 'bib_data' in record_dict:
            record_dict['bib_data'] = decode_field(record_dict['bib_data'], 'bib_data')
        else:
            record_dict['bib_data'] = {}
        
        # Decode metrics if present
        if 'metrics' in record_dict:
            record_dict['metrics'] = decode_field(record_dict['metrics'], 'metrics')
        else:
            record_dict['metrics'] = {}
        
        return record_dict

    def _protobuf_to_dict(self, protobuf_message):
        """
        Convert a protobuf message to a dictionary.
        Handles both single BoostRequestRecord and BoostRequestRecordList messages.
        
        :param protobuf_message: Protobuf message object
        :return: Dictionary representation of the protobuf message
        """
        try:
            # Convert protobuf to dict using MessageToDict
            message_dict = MessageToDict(protobuf_message, preserving_proto_field_name=True)
            
            logger.debug(f"Protobuf message keys before decoding: {list(message_dict.keys())}")
            logger.debug(f"Protobuf message content: {message_dict}")
            
            # Check if this is a BoostRequestRecordList (has 'boost_requests' key)
            if 'boost_requests' in message_dict:
                # This is a list message - decode each record in the list
                boost_requests = message_dict.get('boost_requests', [])
                if not isinstance(boost_requests, list):
                    boost_requests = [boost_requests]
                
                logger.debug(f"Decoding {len(boost_requests)} records in BoostRequestRecordList")
                
                # Decode bib_data and metrics for each record in the list
                decoded_requests = []
                for idx, request in enumerate(boost_requests):
                    try:
                        decoded_request = self._decode_record_fields(request.copy())
                        decoded_requests.append(decoded_request)
                    except Exception as e:
                        logger.error(f"Error decoding record {idx + 1} in boost_requests list: {e}")
                        # Include the original request even if decoding fails
                        decoded_requests.append(request)
                
                message_dict['boost_requests'] = decoded_requests
            else:
                # This is a single request - decode top-level fields
                message_dict = self._decode_record_fields(message_dict)
            
            logger.debug(f"Successfully converted protobuf to dict: {list(message_dict.keys())}")
            return message_dict
            
        except Exception as e:
            logger.error(f"Error converting protobuf to dict: {e}")
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
        # Note: These fields are now properly parsed from JSON strings
        if 'metrics' in record and isinstance(record['metrics'], dict):
            if record['metrics'].get('refereed', False):
                return 1.0
        
        if 'bib_data' in record and isinstance(record['bib_data'], dict):
            if record['bib_data'].get('refereed', False):
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
        logger.debug(f"Record keys: {list(record.keys())}")
        if 'bib_data' in record:
            logger.debug(f"bib_data type: {type(record['bib_data'])}")
            logger.debug(f"bib_data content: {record['bib_data']}")
            doctype = record['bib_data'].get('doctype', '').lower()
        logger.debug(f"Doctype: {doctype}")
        
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

    def _parse_record_date(self, value):
        """
        Parse the date formats that reach us from Master Pipeline.

        Handles the several shapes the same field can take:
          '2022-05-29'                    plain date
          '2022-05-29T00:00:00.000000Z'   ISO timestamp (how master stores entry_date)
          '2022-05-00' / '2022-00-00'     ADS uses '00' for an unknown month or day

        :param value: Date string, or None
        :return: datetime, or None if the value is missing or unparseable
        """
        if not value or not isinstance(value, str):
            return None

        # Drop any time component; we only compare whole days
        text = value.strip().split('T')[0]

        parts = text.split('-')
        year = parts[0]
        month = parts[1] if len(parts) > 1 else '01'
        day = parts[2][:2] if len(parts) > 2 else '01'

        # ADS uses '00' to mean "unknown"; treat it as the first of the period
        month = '01' if month in ('', '00') else month
        day = '01' if day in ('', '00') else day

        try:
            return datetime(int(year), int(month), int(day))
        except (TypeError, ValueError):
            logger.warning(f"Could not parse date {value!r}")
            return None

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
        bib_data = record.get('bib_data')
        if isinstance(bib_data, dict):
            pub_date = bib_data.get('pubdate')
            entry_date = bib_data.get('entry_date')

        # Use the earlier of publication date or entry date. Either may be
        # missing or unparseable; only fall back to 1.0 when neither is usable.
        candidates = [d for d in (self._parse_record_date(pub_date),
                                  self._parse_record_date(entry_date)) if d]
        if not candidates:
            return 1.0

        reference_date = min(candidates)

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

    def _normalize_collections(self, values):
        """
        Normalize raw collection names to the keys used in COLLECTION_RANKINGS.

        Lowercases, strips spaces ('Earth Science' -> 'earthscience') and applies
        COLLECTION_ALIASES ('planetaryscience' -> 'planetary'). Duplicates are
        dropped while preserving order.

        :param values: Iterable of raw collection names
        :return: List of normalized collection names
        """
        aliases = self.config.get('COLLECTION_ALIASES', {})
        normalized = []
        for value in values or []:
            if not value:
                continue
            key = str(value).lower().replace(' ', '')
            key = aliases.get(key, key)
            if key not in normalized:
                normalized.append(key)
        return normalized

    def _resolve_collection_hierarchy(self, collections):
        """
        Resolve umbrella collections against their subcollections.

        'astronomy' encompasses astrophysics, heliophysics and planetary. When a
        record carries the umbrella together with one or more of its
        subcollections, the subcollections are more specific and win, so the
        umbrella is dropped. An umbrella on its own falls back to the first
        subcollection listed in COLLECTION_HIERARCHY.

        :param collections: List of normalized collection names
        :return: List of collection names with umbrellas resolved
        """
        hierarchy = self.config.get('COLLECTION_HIERARCHY', {})
        resolved = list(collections)

        for umbrella, children in hierarchy.items():
            if umbrella not in resolved:
                continue

            resolved.remove(umbrella)
            if any(child in resolved for child in children):
                # A more specific collection is present, so the umbrella adds
                # nothing; drop it.
                logger.debug(f"Dropping umbrella collection '{umbrella}' in favour of "
                             f"{[c for c in children if c in resolved]}")
            elif children:
                # Umbrella on its own; fall back to its primary subcollection
                resolved.append(children[0])

        return resolved

    def compute_collection_weights(self, record):
        """
        Compute collection-based weights for a record based on ranking system

        :param record: Dictionary containing record information
        :return: Dictionary with collection weights
        """
        # Collections come from two sources: 'classifications' (written by the
        # classifier, currently dev only) and 'bib_data.database'. Until
        # classifications ships everywhere we use the UNION of both rather than
        # preferring one, so a record is not under-classified while only one
        # source is populated.
        raw_values = []
        bib_data = record.get('bib_data')
        for source in (record.get('classifications'),
                       bib_data.get('database') if isinstance(bib_data, dict) else None):
            if isinstance(source, str) and source:
                raw_values.append(source)
            elif isinstance(source, (list, tuple)):
                raw_values.extend(source)

        record_collections = self._normalize_collections(raw_values)
        record_collections = self._resolve_collection_hierarchy(record_collections)

        if not record_collections:
            record_collections = ['general']

        logger.debug(f"Record collections: {record_collections}")
        
        # Get ranking configuration from config
        collection_rankings = self.config.get('COLLECTION_RANKINGS', {})
        if not collection_rankings:
            logger.warning("No COLLECTION_RANKINGS found in config, using default weights")
            collections = self.config.get('COLLECTIONS', ['astrophysics', 'physics', 'earthscience', 'planetary', 'heliophysics', 'general'])
            return {f'{collection}_weight': 1.0 for collection in collections}
        
        collections = self.config.get('COLLECTIONS', ['astrophysics', 'physics', 'earthscience', 'planetary', 'heliophysics', 'general'])
        
        # Find all unique ranks that are actually present in the rankings
        all_ranks = set()
        for rankings in collection_rankings.values():
            for rank in rankings.values():
                if rank is not None:
                    all_ranks.add(rank)
        
        if not all_ranks:
            return {f'{collection}_weight': 1.0 for collection in collections}
        
        # Sort ranks and create rank-to-weight mapping
        # Weights are evenly distributed from 1.0 (lowest rank = highest relevance) to 0.1 (highest rank = lowest relevance)
        # This ensures even the lowest relevance gets a small positive weight
        sorted_ranks = sorted(all_ranks)  # Lowest rank first (highest relevance)
        rank_to_weight = {}
        for i, rank in enumerate(sorted_ranks):
            if len(sorted_ranks) == 1:
                # Only one rank, give it weight 1.0
                rank_to_weight[rank] = 1.0
            else:
                # Distribute weights evenly from 1.0 (lowest rank = highest relevance) to 0.1 (highest rank = lowest relevance)
                # This ensures even the lowest relevance gets a small positive weight
                weight = 1.0 - (0.9 * i / (len(sorted_ranks) - 1))
                rank_to_weight[rank] = weight
        
        # For each discipline, find the maximum weight for this discipline across all collections the record belongs to
        collection_weights = {}
        
        for discipline in collections:
            # Find the maximum weight for this discipline across all record collections
            max_weight = 0.0
            for record_collection in record_collections:
                # Look up how relevant this collection is TO the discipline
                discipline_rankings = collection_rankings.get(discipline, {})
                rank = discipline_rankings.get(record_collection)
                if rank is not None:
                    weight = rank_to_weight.get(rank, 0.0)
                    max_weight = max(max_weight, weight)
            
            # Use discipline name directly as column name
            collection_weights[f'{discipline}_weight'] = max_weight
            logger.debug(f"Discipline {discipline}: max_weight = {max_weight}")
        
        logger.debug(f"Final collection weights: {collection_weights}")
        return collection_weights

    def compute_final_boost(self, record):
        """
        Compute all boost factors for a record using the simplified algorithm:
        1. Compute individual boost factors (refereed, doctype, recency)
        2. Compute single boost_factor as weighted average of the three basic boosts
        3. Compute collection weights
        4. Compute discipline final boosts as discipline_weight * boost_factor
        
        :param record: Dictionary containing record information (already parsed)
        :return: Dictionary with all computed boost factors including final boosts
        """
        # Step 1: Compute individual boost factors
        boost_factors = {
            'refereed_boost': self.compute_refereed_boost(record),
            'doctype_boost': self.compute_doctype_boost(record),
            'recency_boost': self.compute_recency_boost(record)
        }
        
        # Step 2: Compute boost_factor as weighted average of doctype, refereed, and recency
        weights = self.config.get('BOOST_WEIGHTS', {})
        if not weights:
            logger.warning("No BOOST_WEIGHTS found in config, using default weights")
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
        
        # Step 3: Compute collection weights
        collection_weights = self.compute_collection_weights(record)
        logger.debug(f"Computed collection weights: {collection_weights}")
        
        # Step 4: Compute all discipline final boosts as discipline_weight * boost_factor
        collections = self.config.get('COLLECTIONS', ['astrophysics', 'physics', 'earthscience', \
            'planetary', 'heliophysics', 'general'])
            
        final_boosts = {}
        for collection in collections:
            # Use collection name directly as column name
            final_boosts[f'{collection}_final_boost'] = collection_weights[f'{collection}_weight'] * boost_factor
        
        # Combine all results into one dictionary
        result = {}
        result.update(boost_factors)  # Individual boost factors
        result.update(collection_weights)  # Collection weights
        result.update(final_boosts)  # Final discipline boosts
        result['boost_factor'] = boost_factor  # Overall boost factor
        
        return result



    # Columns written from a boost_factors dict, in model-attribute order. Every
    # name here is both the BoostFactors column and the compute_final_boost key.
    BOOST_FACTOR_COLUMNS = (
        'refereed_boost',
        'doctype_boost',
        'recency_boost',
        'boost_factor',

        # Collection weights
        'astrophysics_weight',
        'physics_weight',
        'earthscience_weight',
        'planetary_weight',
        'heliophysics_weight',
        'general_weight',

        # Discipline-specific final boosts
        'astrophysics_final_boost',
        'physics_final_boost',
        'earthscience_final_boost',
        'planetary_final_boost',
        'heliophysics_final_boost',
        'general_final_boost',
    )

    def _apply_boost_factors(self, record, boost_factors):
        """
        Copy every boost factor value onto a BoostFactors model instance

        :param record: models.BoostFactors instance
        :param boost_factors: Dictionary of computed boost factors
        """
        for column in self.BOOST_FACTOR_COLUMNS:
            setattr(record, column, boost_factors.get(column))

    def store_boost_factors(self, bibcode, scix_id, boost_factors):
        """
        Store boost factors in database

        :param bibcode: Bibcode
        :param scix_id: SciX ID
        :param boost_factors: Dictionary of computed boost factors
        """
        self.store_boost_factors_batch([{
            'bibcode': bibcode,
            'scix_id': scix_id,
            'boost_factors': boost_factors
        }])

    def store_boost_factors_batch(self, entries):
        """
        Store boost factors for many records in a single session and commit.

        :param entries: List of dicts with 'bibcode', 'scix_id' and 'boost_factors'
        """
        if not entries:
            return

        try:
            with self.session_scope() as session:
                bibcodes = [e['bibcode'] for e in entries if e.get('bibcode')]
                scix_ids = [e['scix_id'] for e in entries if e.get('scix_id')]

                # Fetch every record this batch might update in one query rather
                # than one query per record
                existing_records = []
                if bibcodes or scix_ids:
                    existing_records = session.query(models.BoostFactors).filter(
                        or_(
                            and_(models.BoostFactors.bibcode.in_(bibcodes),
                                 models.BoostFactors.bibcode != None),
                            and_(models.BoostFactors.scix_id.in_(scix_ids),
                                 models.BoostFactors.scix_id != None)
                        )
                    ).all()

                by_bibcode = {r.bibcode: r for r in existing_records if r.bibcode}
                by_scix_id = {r.scix_id: r for r in existing_records if r.scix_id}

                for entry in entries:
                    bibcode = entry.get('bibcode')
                    scix_id = entry.get('scix_id')
                    boost_factors = entry['boost_factors']

                    existing_record = by_bibcode.get(bibcode) or by_scix_id.get(scix_id)

                    if existing_record:
                        self._apply_boost_factors(existing_record, boost_factors)
                        logger.debug(f"Updated boost factors for {bibcode or scix_id}")
                    else:
                        boost_record = models.BoostFactors(bibcode=bibcode, scix_id=scix_id)
                        self._apply_boost_factors(boost_record, boost_factors)
                        session.add(boost_record)

                        # Keep the lookups current so a batch containing the same
                        # record twice updates it instead of inserting a duplicate
                        if bibcode:
                            by_bibcode[bibcode] = boost_record
                        if scix_id:
                            by_scix_id[scix_id] = boost_record
                        logger.debug(f"Created new boost factors for {bibcode or scix_id}")

                session.commit()
                logger.debug("Stored boost factors for %d record(s)", len(entries))

        except Exception as e:
            logger.error(f"Error indexing boost factors: {e}")
            raise

    def _build_boost_response(self, bibcode, scix_id, boost_factors):
        """
        Build the BoostResponseRecord dictionary sent back to Master Pipeline

        The protobuf field names differ from the internal collection names
        (astronomy/earth_science/planetary_science vs
        astrophysics/earthscience/planetary), so the mapping is done here.

        :param bibcode: Bibcode
        :param scix_id: SciX ID
        :param boost_factors: Computed boost factors
        :return: Dictionary matching the BoostResponseRecord schema
        """
        return {
            'bibcode': bibcode,
            'scix_id': scix_id,
            'status': 3,  # Use enum value 3 for updated
            'doctype_boost': boost_factors.get('doctype_boost', 0.0),
            'refereed_boost': boost_factors.get('refereed_boost', 0.0),
            'recency_boost': boost_factors.get('recency_boost', 0.0),
            'boost_factor': boost_factors.get('boost_factor', 0.0),
            'astronomy_final_boost': boost_factors.get('astrophysics_final_boost', 0.0),
            'physics_final_boost': boost_factors.get('physics_final_boost', 0.0),
            'earth_science_final_boost': boost_factors.get('earthscience_final_boost', 0.0),
            'planetary_science_final_boost': boost_factors.get('planetary_final_boost', 0.0),
            'heliophysics_final_boost': boost_factors.get('heliophysics_final_boost', 0.0),
            'general_final_boost': boost_factors.get('general_final_boost', 0.0),
            'created': boost_factors.get('created', datetime.now().isoformat()),
            'modified': boost_factors.get('modified', datetime.now().isoformat())
        }

    def send_to_master_pipeline(self, original_record, boost_factors):
        """
        Send computed boost factors back to Master Pipeline

        :param original_record: Original record from Master Pipeline
        :param boost_factors: Computed boost factors
        """
        try:
            # Parse the original record to get access to parsed data
            parsed_record = self._parse_master_pipeline_message(original_record)

            # Extract bibcode and scix_id from the parsed message
            bibcode = parsed_record.get('bibcode')
            scix_id = parsed_record.get('scix_id')

            if not bibcode:
                logger.error("No bibcode found in parsed record for sending to master pipeline")
                return

            # Create response message with boost factors
            message = self._build_boost_response(bibcode, scix_id, boost_factors)
            protobuf_format = BoostResponseRecord()
            response_message = ParseDict(message, protobuf_format)
            logger.debug(f"Response message: {response_message}")
            logger.debug(f"Response message type: {type(response_message)}")

            # Send to Master Pipeline
            self.forward_message(response_message)

            logger.debug(f"Sent boost factors to Master Pipeline for {bibcode}")

        except Exception as e:
            logger.error(f"Error sending to Master Pipeline: {e}")
            raise

    def send_batch_to_master_pipeline(self, entries):
        """
        Send computed boost factors for many records back to Master Pipeline as a
        single BoostResponseRecordList, rather than one message per record.

        :param entries: List of dicts with 'bibcode', 'scix_id' and 'boost_factors'
        """
        if not entries:
            return

        try:
            responses = [
                self._build_boost_response(entry.get('bibcode'), entry.get('scix_id'),
                                           entry['boost_factors'])
                for entry in entries if entry.get('bibcode')
            ]

            if not responses:
                logger.error("No bibcodes found in batch for sending to master pipeline")
                return

            message = {
                'boost_responses': responses,
                'status': 3  # Use enum value 3 for updated
            }
            response_message = ParseDict(message, BoostResponseRecordList())
            logger.debug(f"Response message type: {type(response_message)}")

            # Send to Master Pipeline
            self.forward_message(response_message)

            logger.info("Sent boost factors for %d record(s) to Master Pipeline",
                        len(responses))

        except Exception as e:
            logger.error(f"Error sending batch to Master Pipeline: {e}")
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
                        'boost_factor': record.boost_factor,

                        # Collection weights
                        'astrophysics_weight': record.astrophysics_weight,
                        'physics_weight': record.physics_weight,
                        'earthscience_weight': record.earthscience_weight,
                        'planetary_weight': record.planetary_weight,
                        'heliophysics_weight': record.heliophysics_weight,
                        'general_weight': record.general_weight,
                        
                        # Discipline-specific final boosts
                        'astrophysics_final_boost': record.astrophysics_final_boost,
                        'physics_final_boost': record.physics_final_boost,
                        'earthscience_final_boost': record.earthscience_final_boost,
                        'planetary_final_boost': record.planetary_final_boost,
                        'heliophysics_final_boost': record.heliophysics_final_boost,
                        'general_final_boost': record.general_final_boost,
                        
                        'created': record.created.isoformat() if record.created else None
                    }
                    for record in records
                ]
                
        except Exception as e:
            logger.error(f"Error querying boost factors: {e}")
            raise 