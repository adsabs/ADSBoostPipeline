#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import json
import argparse
import csv
from adsputils import load_config, setup_logging
from sqlalchemy.orm import load_only
import ADSBoost.tasks as tasks

# ============================= INITIALIZATION ==================================== #
proj_home = os.path.realpath(os.path.dirname(__file__))
config = load_config(proj_home=proj_home)

logger = setup_logging('run.py', proj_home=proj_home,
                       level=config.get('LOGGING_LEVEL', 'DEBUG'),
                       attach_stdout=config.get('LOG_STDOUT', False))

app = tasks.app

# =============================== FUNCTIONS ======================================= #

def process_file(file_path):
    """
    Process records from a file using Celery tasks
    """
    logger.info(f"Processing records from file: {file_path}")
    
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return
    
    try:
        with open(file_path, 'r') as f:
            if file_path.endswith('.json'):
                records = json.load(f)
            elif file_path.endswith('.csv'):
                records = []
                reader = csv.DictReader(f)
                for row in reader:
                    records.append(row)
            else:
                logger.error("Unsupported file format. Use JSON or CSV.")
                return
        
        logger.info(f"Processing {len(records)} records from file using Celery tasks")

        # Process records in batches
        process_batch(records)

    except Exception as e:
        logger.error(f"Error processing file: {e}")

def query_boost_factors(app, query_id, logger):
    """
    Query boost factors for a specific record
    """
    logger.info(f"Querying boost factors for: {query_id}")
    
    try:
        # Try as bibcode first, then as scix_id
        results = app.query_boost_factors(bibcode=query_id)
        if not results:
            results = app.query_boost_factors(scix_id=query_id)
        
        if results:
            for result in results:
                logger.info(f"Boost factors retrieved for {query_id}:")
        else:
            logger.info(f"No boost factors found for {query_id}")
            
    except Exception as e:
        logger.error(f"Error querying boost factors: {e}")

def export_boost_factors(app, output_path, logger):
    """
    Export boost factors to CSV with the new structure
    """
    logger.info(f"Exporting boost factors to: {output_path}")
    
    try:
        # Create CSV file with headers
        with open(output_path, 'w', newline='') as csvfile:
            fieldnames = [
                'bibcode', 'scix_id', 'created',
                'doctype_boost', 'refereed_boost', 'recency_boost', 'boost_factor',
                'astrophysics_weight', 'physics_weight', 'earthscience_weight',
                'planetary_weight', 'heliophysics_weight', 'general_weight',
                'astrophysics_final_boost', 'physics_final_boost', 'earthscience_final_boost',
                'planetary_final_boost', 'heliophysics_final_boost', 'general_final_boost'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            with app.session_scope() as session:
                records = session.query(app.models.BoostFactors).all()
                
                for record in records:
                    record_dict = {
                        'bibcode': record.bibcode,
                        'scix_id': record.scix_id,
                        'created': record.created.isoformat() if record.created else None,
                        'doctype_boost': record.doctype_boost,
                        'refereed_boost': record.refereed_boost,
                        'recency_boost': record.recency_boost,
                        'boost_factor': record.boost_factor,
                        'astrophysics_weight': record.astrophysics_weight,
                        'physics_weight': record.physics_weight,
                        'earthscience_weight': record.earthscience_weight,
                        'planetary_weight': record.planetary_weight,
                        'heliophysics_weight': record.heliophysics_weight,
                        'general_weight': record.general_weight,
                        'astrophysics_final_boost': record.astrophysics_final_boost,
                        'physics_final_boost': record.physics_final_boost,
                        'earthscience_final_boost': record.earthscience_final_boost,
                        'planetary_final_boost': record.planetary_final_boost,
                        'heliophysics_final_boost': record.heliophysics_final_boost,
                        'general_final_boost': record.general_final_boost
                    }
                    writer.writerow(record_dict)
        
        logger.info(f"Successfully exported {len(records)} records to {output_path}")
        
    except Exception as e:
        logger.error(f"Error exporting boost factors: {e}")

def process_batch(records_batch):
    """
    Process a batch of records for boost factor computation using Celery tasks
    
    :param records_batch: List of record dictionaries
    """
    logger.info(f"Processing batch of {len(records_batch)} records using Celery tasks")
    
    try:
        # Submit all records to Celery tasks for processing
        _tasks = []
        
        for i, record in enumerate(records_batch):
            try:
                bibcode = record.get('bibcode', '')
                logger.debug(f"Submitting record {i+1}/{len(records_batch)}: {bibcode} to Celery")
                
                # Submit compute task
                t = tasks.task_compute_boost_factors.delay(record)
                _tasks.append(t)
                
            except Exception as e:
                logger.error(f"Error submitting record {i+1} to Celery: {e}")
                # Continue with next record instead of failing entire batch
                continue
        
        logger.info(f"Submitted {len(tasks)} records to Celery for processing")
                        
    except Exception as e:
        logger.error(f"Error processing batch through Celery: {e}")
        raise

def main():
    """
    Main entry point for the Boost Pipeline
    """
    parser = argparse.ArgumentParser(description='ADS Boost Pipeline')
    parser.add_argument('-f', '--filename', help='Input file with records to process')
    parser.add_argument('-b', '--bibcodes', help='Process single OR multiple records by bibcode')
    parser.add_argument('-x', '--scix_id', help='Process single OR multiple records by SciX ID')
    parser.add_argument('-q', '--query', help='Query boost factors by bibcode or scix_id')
    parser.add_argument('-e', '--export', help='Export boost factors to CSV file')
    parser.add_argument('-d', '--debug', action='store_true', help='Debug mode')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose output')    
    
    args = parser.parse_args()
    
    # Setup
    proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), "."))
    config = load_config(proj_home=proj_home)
    
    log_level = 'DEBUG' if args.debug else 'INFO'
    logger = setup_logging('run.py', proj_home=proj_home,
                          level=log_level,
                          attach_stdout=config.get('LOG_STDOUT', True))
    
    try:
        if args.filename:
            logger.info("Processing bibcodes from file")
            process_file(args.filename)
            
        elif args.bibcodes:
            logger.info(f"Processing {len(args.bibcodes)} bibcodes from command line")
            process_batch(args.bibcodes)
                       
        elif args.scix_id:
            # Single or multiple scix_ids entered as command line arguments
            if isinstance(args.scix_id, str):
                args.scix_id = [args.scix_id]
            logger.info(f"Processing {len(args.scix_id)} scix_ids from command line")
            process_batch(args.scix_id)             
        elif args.query:
            query_boost_factors(args.query)
        elif args.export:
            export_boost_factors(args.export)
        else:
            logger.info("No arguments provided. Starting Boost Pipeline in listening mode...")
            
    except KeyboardInterrupt:
        logger.info("Shutting down Boost Pipeline...")
    except Exception as e:
        logger.error(f"Error in main: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main() 