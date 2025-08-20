#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import json
import argparse
import csv
from adsputils import load_config, setup_logging
from sqlalchemy.orm import load_only
from ADSBoost.app import ADSBoostCelery
from ADSBoost.tasks import (
    task_compute_boost_factors,
    task_store_boost_factors,
    task_send_to_master_pipeline
)

def process_file(app, file_path, logger):
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
        
        # Convert to bibcodes list and use batch processing
        bibcodes = []
        for record in records:
            bibcode = record.get('bibcode') or record.get('scix_id')
            if bibcode:
                bibcodes.append(bibcode)
        
        if bibcodes:
            process_bibcodes_in_batches(app, bibcodes, logger, batch_size=100)
        else:
            logger.warning("No valid bibcodes found in file")
                
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
                logger.info(f"Boost factors retrived for {query_id}:")
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
                'astronomy_weight', 'physics_weight', 'earth_science_weight',
                'planetary_science_weight', 'heliophysics_weight', 'general_weight',
                'astronomy_final_boost', 'physics_final_boost', 'earth_science_final_boost',
                'planetary_science_final_boost', 'heliophysics_final_boost', 'general_final_boost'
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
                        'general_final_boost': record.general_final_boost
                    }
                    writer.writerow(record_dict)
        
        logger.info(f"Successfully exported {len(records)} records to {output_path}")
        
    except Exception as e:
        logger.error(f"Error exporting boost factors: {e}")

def process_all_records_from_master(app, logger, batch_size=100):
    """
    Process ALL records from Master Pipeline's Records database
    
    This function queries the Master Pipeline database and processes every record
    to compute boost factors. This is useful for initial setup or rebuilding.
    
    :param app: ADSBoostCelery app instance
    :param logger: Logger instance
    :param batch_size: Number of records to process in each batch
    """
    logger.info(f"Starting to process ALL records from Master Pipeline database with batch size: {batch_size}")
    
    try:
        # Check if we have access to Master Pipeline database
        if not hasattr(app, 'master_session_scope'):
            logger.error("No access to Master Pipeline database. Cannot process all records.")
            return
        
        sent = 0
        batch = []
        
        with app.master_session_scope() as session:
            # Query all records from Master Pipeline's Records table
            for rec in session.query(app.master_models.Records) \
                            .options(load_only(app.master_models.Records.bibcode, app.master_models.Records.bib_data)) \
                            .yield_per(batch_size):
                
                sent += 1
                if sent % 1000 == 0:
                    logger.info(f"Processed {sent} records so far...")
                
                # Extract bibcode and create record structure
                bibcode = rec.bibcode
                if not bibcode:
                    continue
                
                # Create record structure that Boost Pipeline expects
                record_data = {
                    'bibcode': bibcode,
                    'scix_id': getattr(rec, 'scix_id', None),
                    'bib_data': rec.bib_data if rec.bib_data else {},
                    'metrics': {},  # Will be populated if available
                    'classifications': getattr(rec, 'classifications', [])
                }
                
                batch.append(record_data)
                
                if len(batch) >= batch_size:
                    logger.info(f"Processing batch of {len(batch)} records")
                    process_batch(app, batch, logger)
                    batch = []
        
        # Process remaining records in final batch
        if batch:
            logger.info(f"Processing final batch of {len(batch)} records")
            process_batch(app, batch, logger)
        
        logger.info(f"Completed processing ALL records. Total processed: {sent}")
        
    except Exception as e:
        logger.error(f"Error processing all records: {e}")
        raise

def process_batch(app, records_batch, logger):
    """
    Process a batch of records for boost factor computation using Celery tasks
    
    :param app: ADSBoostCelery app instance
    :param records_batch: List of record dictionaries
    :param logger: Logger instance
    """
    logger.info(f"Processing batch of {len(records_batch)} records using Celery tasks")
    
    try:
        # Submit all records to Celery tasks for processing
        tasks = []
        
        for i, record in enumerate(records_batch):
            try:
                bibcode = record.get('bibcode', record.get('scix_id'))
                logger.debug(f"Submitting record {i+1}/{len(records_batch)}: {bibcode} to Celery")
                
                # Submit compute task
                compute_task = task_compute_boost_factors.delay(record)
                tasks.append({
                    'record': record,
                    'compute_task': compute_task,
                    'bibcode': bibcode,
                    'index': i
                })
                
            except Exception as e:
                logger.error(f"Error submitting record {i+1} to Celery: {e}")
                # Continue with next record instead of failing entire batch
                continue
        
        logger.info(f"Submitted {len(tasks)} records to Celery for processing")
        
        # Wait for compute tasks to complete and then submit store/send tasks
        for task_info in tasks:
            try:
                # Wait for compute task to complete
                boost_factors = task_info['compute_task'].get(timeout=300)  # 5 minute timeout
                record = task_info['record']
                bibcode = task_info['bibcode']
                
                logger.debug(f"Compute task completed for {bibcode}, submitting store and send tasks")
                
                # Submit store task
                store_task = task_store_boost_factors.delay(
                    record.get('bibcode'), 
                    record.get('scix_id'), 
                    boost_factors
                )
                
                # Submit send to master pipeline task if configured
                if app.config.get('DELAY_MESSAGE', True):
                    send_task = task_send_to_master_pipeline.delay(record, boost_factors)
                    logger.debug(f"Submitted send task for {bibcode}")
                
                logger.debug(f"Successfully processed {bibcode} through Celery pipeline")
                
            except Exception as e:
                logger.error(f"Error processing record {task_info['index']+1} through Celery: {e}")
                # Continue with next record instead of failing entire batch
                continue
        
        logger.info(f"Completed processing batch of {len(records_batch)} records through Celery")
        
    except Exception as e:
        logger.error(f"Error processing batch through Celery: {e}")
        raise

def process_bibcodes_in_batches(app, bibcodes, logger, batch_size=100):
    """
    Process a list of bibcodes in batches using Celery tasks
    
    :param app: ADSBoostCelery app instance
    :param bibcodes: List of bibcode strings
    :param logger: Logger instance
    :param batch_size: Number of bibcodes to process in each batch
    """
    if not bibcodes:
        logger.warning("No bibcodes provided for processing")
        return
    
    logger.info(f"Processing {len(bibcodes)} bibcodes in batches of {batch_size} using Celery tasks")
    
    try:
        # Process bibcodes in batches
        sent = 0
        batch = []
        
        for bibcode in bibcodes:
            sent += 1
            if sent % 1000 == 0:
                logger.info(f"Processed {sent} bibcodes so far...")
            
            # Create minimal record structure for boost computation
            record_data = {
                'bibcode': bibcode,
                'scix_id': None,  # Will be populated if available
                'bib_data': {},    # Will be populated from Master Pipeline if available
                'metrics': {},
                'classifications': []
            }
            
            batch.append(record_data)
            
            if len(batch) >= batch_size:
                logger.info(f"Processing batch of {len(batch)} bibcodes through Celery")
                process_batch(app, batch, logger)
                batch = []
        
        # Process remaining bibcodes in final batch
        if batch:
            logger.info(f"Processing final batch of {len(batch)} bibcodes through Celery")
            process_batch(app, batch, logger)
        
        logger.info(f"Completed processing {sent} bibcodes through Celery")
        
    except Exception as e:
        logger.error(f"Error processing bibcodes in batches through Celery: {e}")
        raise

def read_bibcodes_from_file(filename, logger):
    """
    Read bibcodes from a file (one per line)
    
    :param filename: Path to file containing bibcodes
    :param logger: Logger instance
    :return: List of bibcode strings
    """
    logger.info(f"Reading bibcodes from file: {filename}")
    
    if not os.path.exists(filename):
        logger.error(f"File not found: {filename}")
        return []
    
    try:
        bibcodes = []
        
        # Read bibcodes from file (one per line)
        with open(filename, 'r') as f:
            for line in f:
                bibcode = line.strip()
                if bibcode and not bibcode.startswith('#'):  # Skip empty lines and comments
                    bibcodes.append(bibcode)
        
        logger.info(f"Found {len(bibcodes)} bibcodes in file")
        return bibcodes
        
    except Exception as e:
        logger.error(f"Error reading bibcodes from file: {e}")
        return []

def process_all_records_from_master(app, logger, batch_size=100):
    """
    Process ALL records from Master Pipeline's Records database
    
    This function queries the Master Pipeline database and processes every record
    to compute boost factors. This is useful for initial setup or rebuilding.
    
    :param app: ADSBoostCelery app instance
    :param logger: Logger instance
    :param batch_size: Number of records to process in each batch
    """
    logger.info(f"Starting to process ALL records from Master Pipeline database with batch size: {batch_size}")
    
    try:
        # Check if we have access to Master Pipeline database
        if not hasattr(app, 'master_session_scope'):
            logger.error("No access to Master Pipeline database. Cannot process all records.")
            return
        
        sent = 0
        batch = []
        
        with app.master_session_scope() as session:
            # Query all records from Master Pipeline's Records table
            for rec in session.query(app.master_models.Records) \
                            .options(load_only(app.master_models.Records.bibcode, app.master_models.Records.bib_data)) \
                            .yield_per(batch_size):
                
                sent += 1
                if sent % 1000 == 0:
                    logger.info(f"Processed {sent} records so far...")
                
                # Extract bibcode and create record structure
                bibcode = rec.bibcode
                if not bibcode:
                    continue
                
                # Create record structure that Boost Pipeline expects
                record_data = {
                    'bibcode': bibcode,
                    'scix_id': getattr(rec, 'scix_id', None),
                    'bib_data': rec.bib_data if rec.bib_data else {},
                    'metrics': {},  # Will be populated if available
                    'classifications': getattr(rec, 'classifications', [])
                }
                
                batch.append(record_data)
                
                if len(batch) >= batch_size:
                    logger.info(f"Processing batch of {len(batch)} records")
                    process_batch(app, batch, logger)
                    batch = []
        
        # Process remaining records in final batch
        if batch:
            logger.info(f"Processing final batch of {len(batch)} records")
            process_batch(app, batch, logger)
        
        logger.info(f"Completed processing ALL records. Total processed: {sent}")
        
    except Exception as e:
        logger.error(f"Error processing all records: {e}")
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
    parser.add_argument('--process-all', action='store_true', 
                       help='Process ALL records from Master Pipeline database')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='Batch size for processing multiple records (default: 100)')
    parser.add_argument('--filename', help='File containing list of bibcodes (one per line)')
    
    args = parser.parse_args()
    
    # Setup
    proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), "."))
    config = load_config(proj_home=proj_home)
    
    log_level = 'DEBUG' if args.debug else 'INFO'
    logger = setup_logging('run.py', proj_home=proj_home,
                          level=log_level,
                          attach_stdout=config.get('LOG_STDOUT', True))
    
    app = ADSBoostCelery('ADSBoostPipeline', proj_home=proj_home, local_config=config)
    
    try:
        if args.process_all:
            # Feature 1: Process ALL records from Master Pipeline database
            logger.info("Processing ALL records from Master Pipeline database")
            process_all_records_from_master(app, logger, batch_size=args.batch_size)
            
        elif args.filename:
            # Feature 3: Extract bibcodes from file and batch process
            logger.info("Processing bibcodes from file")
            bibcodes = read_bibcodes_from_file(args.filename, logger)
            if bibcodes:
                process_bibcodes_in_batches(app, bibcodes, logger, batch_size=args.batch_size)
            
        elif args.bibcodes:
            # Batch process single or multiple bibcodes from command line
            if isinstance(args.bibcodes, str):
                args.bibcodes = [args.bibcodes]
            logger.info(f"Processing {len(args.bibcodes)} bibcodes from command line")
            process_bibcodes_in_batches(app, args.bibcodes, logger, batch_size=args.batch_size)
                       
        elif args.scix_id:
            # Single or multiple scix_ids entered as command line arguments
            if isinstance(args.scix_id, str):
                args.scix_id = [args.scix_id]
            logger.info(f"Processing {len(args.scix_id)} scix_ids from command line")
            process_bibcodes_in_batches(app, args.scix_id, logger, batch_size=args.batch_size)
            
        elif args.file:
            process_file(app, args.file, logger)
        elif args.query:
            query_boost_factors(app, args.query, logger)
        elif args.export:
            export_boost_factors(app, args.export, logger)
        else:
            # Start the pipeline in listening mode
            logger.info("Starting Boost Pipeline in listening mode...")
            app.start()
            
    except KeyboardInterrupt:
        logger.info("Shutting down Boost Pipeline...")
    except Exception as e:
        logger.error(f"Error in main: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main() 