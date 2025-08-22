# -*- coding: utf-8 -*-

import os
import json
import logging
from adsputils import load_config, setup_logging
from adsboost import app as app_module
from kombu import Queue
# ============================= INITIALIZATION ==================================== #

# Setup logging
proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), "../"))

# Create Celery app
app = app_module.ADSBoostCelery('boost-pipeline', proj_home=proj_home, 
                        local_config=globals().get('local_config', {}))
logger = app.logger

app.conf.CELERY_QUEUES = (
    Queue('update-record', app.exchange, routing_key='update-record'),
    Queue('compute-boost', app.exchange, routing_key='compute-boost'),
    Queue('send-boost-response', app.exchange, routing_key='send-boost-response'),
    Queue('export-boost', app.exchange, routing_key='export-boost'),

)


# ============================= TASKS ============================================= #



@app.task(queue='update-record')
def task_process_boost_request_message(message, pipeline='boost'):
    """
    Process a boost request message from the Master Pipeline
    
    :param message: JSON string containing boost request message
    """
    try:
        logger.info("Processing boost request message")
        app.handle_message_payload(message=message)
        return "success"
    except Exception as e:
        logger.error(f"Error processing boost request message: {e}")
        raise

@app.task(queue='compute-boost')
def task_compute_boost_factors(record_data):
    """
    Compute boost factors for a single record using the simplified algorithm:
    1. Compute doctype_boost, refereed_boost, recency_boost
    2. Compute boost_factor as weighted average of the three basic boosts
    3. Compute collection weights based on discipline rankings
    4. Compute discipline-specific final boosts as discipline_weight * boost_factor
    
    :param record_data: Dictionary containing record information
    :return: Dictionary with computed boost factors including boost_factor and discipline final boosts
    """
    try:
        logger.info(f"Computing boost factors for {record_data.get('bibcode', record_data.get('scix_id'))}")
        boost_factors = app.compute_final_boost(record_data)

        # Extract bibcode and scix_id from record_data
        bibcode = record_data.get('bibcode')
        scix_id = record_data.get('scix_id')
        
        if bibcode or scix_id:
            task_store_boost_factors(bibcode, scix_id, boost_factors)
            
        return boost_factors
    except Exception as e:
        logger.error(f"Error computing boost factors: {e}")
        raise

def task_query_boost_factors(bibcode=None, scix_id=None):
    """
    Query boost factors from the database
    
    :param bibcode: Bibcode to query
    :param scix_id: SciX ID to query
    :return: List of boost factor records with new structure
    """
    try:
        logger.info(f"Querying boost factors for {bibcode or scix_id}")
        results = app.query_boost_factors(bibcode=bibcode, scix_id=scix_id)
        return results
    except Exception as e:
        logger.error(f"Error querying boost factors: {e}")
        raise

@app.task(queue='export-boost')
def task_export_boost_factors(output_path, bibcodes=None, scix_ids=None):
    """
    Export boost factors to a CSV file with the new structure
    
    :param output_path: Path to output CSV file
    :param bibcodes: List of bibcodes to export (optional)
    :param scix_ids: List of scix_ids to export (optional)
    """
    try:
        logger.info(f"Exporting boost factors to {output_path}")
        
        # Prepare output file
        app.prepare_output_file(output_path)
        
        # Query all records if no specific IDs provided
        if not bibcodes and not scix_ids:
            with app.session_scope() as session:
                records = session.query(app.models.BoostFactors).all()
                for record in records:
                    record_dict = {
                        'bibcode': record.bibcode,
                        'scix_id': record.scix_id,
                        'doctype_boost': record.doctype_boost,
                        'refereed_boost': record.refereed_boost,
                        'recency_boost': record.recency_boost,
                        'boost_factor': record.boost_factor,
                        'astronomy_weight': record.astronomy_weight,
                        'physics_weight': record.physics_weight,
                        'earth_science_weight': record.earth_science_weight,
                        'planetary_science_weight': record.planetary_science_weight,
                        'heliophysics_weight': record.heliophysics_weight,
                        'general_weight': record.general_weight,
                        'astronomy_final_boost': record.astronomy_final_boost,
                        'physics_final_boost': record.physics_final_boost,
                        'earth_science_final_boost': record.earth_science_final_boost,
                        'planetary_science_final_boost': record.planetary_science_final_boost,
                        'heliophysics_final_boost': record.heliophysics_final_boost,
                        'general_final_boost': record.general_final_boost,
                        'created': record.created.isoformat() if record.created else None
                    }
                    app.add_record_to_output_file(record_dict, output_path)
        else:
            # Query specific records
            if bibcodes:
                for bibcode in bibcodes:
                    results = app.query_boost_factors(bibcode=bibcode)
                    for result in results:
                        app.add_record_to_output_file(result, output_path)
            
            if scix_ids:
                for scix_id in scix_ids:
                    results = app.query_boost_factors(scix_id=scix_id)
                    for result in results:
                        app.add_record_to_output_file(result, output_path)
        
        logger.info(f"Successfully exported boost factors to {output_path}")
        return {"status": "success", "output_path": output_path}
        
    except Exception as e:
        logger.error(f"Error exporting boost factors: {e}")
        raise

def task_store_boost_factors(bibcode, scix_id, boost_factors):
    """Store computed boost factors in database"""
    try:
        app.store_boost_factors(bibcode, scix_id, boost_factors)
        return "success"
    except Exception as e:
        logger.error(f"Error storing boost factors: {e}")
        raise

@app.task(queue='send-boost-response')
def task_send_to_master_pipeline(original_record, boost_factors):
    """Send computed boost factors back to Master Pipeline"""
    try:
        app.send_to_master_pipeline(original_record, boost_factors)
        return "success"
    except Exception as e:
        logger.error(f"Error sending to Master Pipeline: {e}")
        raise

if __name__ == '__main__':
    app.start() 