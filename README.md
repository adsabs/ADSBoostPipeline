# ADS Boost Pipeline

A Celery-based pipeline for computing and storing discipline-specific search boost factors for the NASA ADS search engine. This pipeline calculates boost factors based on various criteria to prioritize better results in search rankings across different scientific disciplines.

## Overview

The Boost Pipeline computes boost factors for ADS records based on:
- **Refereed status**: Refereed publications get higher priority
- **Document type**: Different document types have configurable ranking-based boost factors
- **Recency**: Recent publications are prioritized with configurable decay functions
- **Collection weights**: Discipline-specific weights based on collection rankings
- **Final discipline boosts**: Combined boost factors for each scientific discipline

## Architecture

The pipeline follows the standard ADS pipeline architecture:
- **Celery-based**: Asynchronous task processing with multiple queues
- **Database storage**: PostgreSQL for storing boost factors with comprehensive schema
- **Message passing**: RabbitMQ for bidirectional communication with Master Pipeline
- **Modular design**: Separate components for computation, storage, and communication

## Data Flow

### 1. Master Pipeline → Boost Pipeline Flow
- **Trigger**: When Master Pipeline processes any record type (metadata, metrics, etc.), it automatically generates a boost request
- **Message Format**: JSON message containing:
  - `bibcode` and `scix_id` at root level
  - `bib_data` section (paper metadata, title, abstract, etc.)
  - `metrics` section (refereed status, citations)
  - `classifications` section (collections/disciplines)
- **Transport**: Celery message queue system

### 2. Boost Pipeline Processing
- **Input**: Receives boost request messages from Master Pipeline
- **Processing**: Computes multiple boost factors:
  - `refereed_boost`: 1.0 for refereed papers, 0.0 for non-refereed
  - `doctype_boost`: Based on document type ranking system (configurable)
  - `recency_boost`: Time-based decay function (24-month cutoff)
- **Collection Weights**: Computes discipline-specific weights based on collection rankings
- **Final Boosts**: Combines basic boosts into discipline-specific final boost scores

### 3. Output Flow
- **Storage**: Saves computed boost factors to PostgreSQL database
- **Response**: Sends boost factors back to Master Pipeline via Celery
- **Integration**: Master Pipeline stores boost factors in its `boost_factors` field

## Boost Factor Computation Algorithm

### Basic Boost Factors
1. **Refereed Boost**: 1.0 for refereed papers, 0.0 for non-refereed
2. **Document Type Boost**: Based on configurable ranking system
3. **Recency Boost**: Reciprocal decay function with 24-month cutoff

### Collection Weight System
- Supports 6 disciplines: astronomy, physics, earth_science, planetary_science, heliophysics, general
- Uses ranking system to assign relevance weights (1.0 to 0.1)
- Handles multiple collections per record by taking maximum weight per discipline

### Final Calculation
1. **Combined Boost Factor**: Weighted average of basic boost factors
2. **Discipline Final Boosts**: `discipline_weight × combined_boost_factor`

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd ADSBoostPipeline
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up database:
```bash
# Create database
createdb boostfactorsdb

# Run migrations
alembic upgrade head
```

4. Configure RabbitMQ:
```bash
# Create queues
rabbitmqctl add_user test test
rabbitmqctl set_permissions test ".*" ".*" ".*"
```

## Configuration

Edit `config.py` to configure:
- Database connection (`SQLALCHEMY_URL`)
- RabbitMQ settings (`CELERY_BROKER_URL`)
- Boost factor parameters:
  - `DOCTYPE_RANKING`: Document type importance rankings
  - `COLLECTION_RANKINGS`: Discipline relevance rankings
  - `BOOST_FACTOR_WEIGHTS`: Weights for basic boost factors
  - `RECENCY_BOOST_MULTIPLIER`: Time decay parameters
- Logging levels (`LOGGING_LEVEL`)

## Usage

### Command Line Interface

#### Process a single record:
```bash
python run.py -b 2023ApJ...123..456T
python run.py -x scix_id_123
```

#### Process records from file:
```bash
python run.py -f records.json
python run.py -f records.csv
```

#### Query boost factors:
```bash
python run.py -q 2023ApJ...123..456T
```

#### Export boost factors:
```bash
python run.py -e output.csv
```

#### Start in listening mode:
```bash
python run.py
```

### Unified Batch Processing Features

The pipeline uses a single, unified batch processing function that handles all bibcode processing scenarios through Celery tasks. Whether processing one bibcode or millions, everything goes through the same efficient, asynchronous Celery task pipeline.

#### 1. Process ALL Records from Master Pipeline Database
```bash
python run.py --process-all --batch-size 500
```
- **Purpose**: Queries the Master Pipeline's Records database and processes every single record
- **Use Case**: Initial setup, rebuilding boost factors, or processing entire database
- **Features**: 
  - Configurable batch size (default: 100)
  - Progress logging every 1000 records
  - Automatic record structure creation
  - Error handling that continues processing
  - **Uses Celery tasks for asynchronous processing**

#### 2. Batch Process Multiple Bibcodes from Command Line
```bash
python run.py --bibcodes-batch 2023ApJ...123A 2023MNRAS.456B 2023A&A...789C
```
- **Purpose**: Processes multiple bibcodes provided as command-line arguments
- **Use Case**: Quick processing of a small set of specific records
- **Features**:
  - Variable number of bibcodes
  - Automatic record structure creation
  - Individual error handling per record
  - **Uses Celery tasks for asynchronous processing**

#### 3. Extract Bibcodes from File and Batch Process
```bash
python run.py --filename bibcodes.txt --batch-size 200
```
- **Purpose**: Reads bibcodes from a text file (one per line) and processes them in batches
- **Use Case**: Processing large lists of bibcodes from external sources
- **Features**:
  - Supports text files with one bibcode per line
  - Skips empty lines and comments (lines starting with #)
  - Configurable batch size
  - Progress logging every 1000 bibcodes
  - **Uses Celery tasks for asynchronous processing**

#### 4. Single Bibcode Processing (Batch of 1)
```bash
python run.py -b 2023ApJ...123..456T
python run.py -x scix_id_123
```
- **Purpose**: Process individual bibcodes or scix_ids
- **Implementation**: Automatically treated as a batch of 1 for consistency
- **Benefits**: Same error handling and logging as batch processing
- **Uses Celery tasks for asynchronous processing**

### Advanced Batch Processing Examples

```bash
# Process all records in Master Pipeline database with 500-record batches
python run.py --process-all --batch-size 500

# Process bibcodes from file with 200-record batches
python run.py --filename my_bibcodes.txt --batch-size 200

# Process specific bibcodes from command line
python run.py --bibcodes-batch 2023ApJ...123A 2023MNRAS.456B

# Process single bibcode (automatically uses batch processing)
python run.py -b 2023ApJ...123..456T

# Export existing boost factors to CSV
python run.py --export boost_factors.csv

# Query boost factors for a specific record
python run.py --query 2023ApJ...123A
```

### Batch Processing Architecture

The pipeline uses a unified approach where all bibcode processing flows through the same Celery task pipeline:

1. **`read_bibcodes_from_file()`**: Extracts bibcodes from text files
2. **`process_bibcodes_in_batches()`**: Main batch processing function that submits to Celery
3. **`process_batch()`**: Submits records to Celery tasks and manages the task pipeline
4. **`process_all_records_from_master()`**: Special case for processing entire Master Pipeline database

**Celery Task Flow:**
- **Compute Task**: `task_compute_boost_factors.delay(record)` - Computes boost factors
- **Store Task**: `task_store_boost_factors.delay(bibcode, scix_id, boost_factors)` - Stores in database
- **Send Task**: `task_send_to_master_pipeline.delay(record, boost_factors)` - Sends to Master Pipeline

This design eliminates code duplication, ensures consistent behavior across all processing scenarios, and properly leverages Celery for asynchronous, scalable processing whether handling 1 record or 1 million records.

## Database Schema

The pipeline uses a comprehensive `boost_factors` table with the following structure:

### Core Fields
- `id`: Primary key
- `bibcode`: Bibcode (19 characters)
- `scix_id`: SciX ID (19 characters)
- `created`: Timestamp

### Basic Boost Factors
- `refereed_boost`: Refereed status boost factor
- `doctype_boost`: Document type boost factor
- `recency_boost`: Recency boost factor
- `boost_factor`: Combined weighted average of basic boosts

### Collection Weights
- `astronomy_weight`: Astronomy discipline weight
- `physics_weight`: Physics discipline weight
- `earth_science_weight`: Earth science discipline weight
- `planetary_science_weight`: Planetary science discipline weight
- `heliophysics_weight`: Heliophysics discipline weight
- `general_weight`: General discipline weight

### Final Discipline Boosts
- `astronomy_final_boost`: Final astronomy boost score
- `physics_final_boost`: Final physics boost score
- `earth_science_final_boost`: Final earth science boost score
- `planetary_science_final_boost`: Final planetary science boost score
- `heliophysics_final_boost`: Final heliophysics boost score
- `general_final_boost`: Final general boost score

## Celery Task Structure

The pipeline uses multiple Celery queues for different operations:

### Task Queues
- `boost-request`: Process incoming boost request messages
- `compute-boost`: Compute boost factors for records
- `store-boost`: Store computed boost factors in database
- `send-boost-response`: Send boost factors back to Master Pipeline
- `export-boost`: Export boost factors to CSV

### Task Functions
- `task_process_boost_request_message`: Main entry point for boost requests
- `task_compute_boost_factors`: Compute boost factors for a record
- `task_store_boost_factors`: Store boost factors in database
- `task_send_to_master_pipeline`: Send response to Master Pipeline
- `task_export_boost_factors`: Export data to CSV

## Integration with Master Pipeline

The Boost Pipeline integrates with the ADS Master Pipeline by:

### Message Exchange
1. **Incoming**: Receives boost requests via `boost-request` queue
2. **Processing**: Computes boost factors using the simplified algorithm
3. **Storage**: Stores results in PostgreSQL database
4. **Response**: Sends computed boost factors back via `send-boost-response` queue

### Data Synchronization
- Automatic boost factor computation when records are updated
- Bidirectional communication for real-time processing
- Database integration for persistent storage

## Performance Characteristics

- **Throughput**: Configurable batch processing (default: 100 records/batch)
- **Memory usage**: Efficient batch processing with configurable sizes
- **Database**: Optimized queries with indexes on bibcode and scix_id
- **Scalability**: Multiple Celery workers can process different queues simultaneously

## Development

### Running Tests
```bash
pytest tests/
```

### Code Style
The project follows PEP 8 guidelines.

### Adding New Boost Factors

To add new boost factors:

1. Update the `compute_boost_factors` method in `app.py`
2. Add corresponding database columns in `models.py`
3. Update the configuration in `config.py`
4. Run database migrations

## Monitoring and Logging

The pipeline provides comprehensive logging:
- Request processing logs at multiple levels
- Boost factor computation logs with detailed breakdowns
- Database operation logs
- Error tracking with full stack traces
- Progress logging for batch operations

### Log Levels
- `DEBUG`: Detailed computation steps and data structures
- `INFO`: Processing progress and successful operations
- `WARNING`: Configuration issues and non-critical problems
- `ERROR`: Processing failures and system errors

## Troubleshooting

### Common Issues

1. **Database connection errors**: Check `SQLALCHEMY_URL` in config
2. **RabbitMQ connection errors**: Verify RabbitMQ is running and accessible
3. **Import errors**: Ensure all dependencies are installed
4. **Batch processing failures**: Check batch size configuration and memory limits

### Logs

Check logs for detailed error information:
```bash
tail -f logs/adsboost_pipeline.log
```

### Configuration Validation

The pipeline validates required configuration parameters:
- Database connection strings
- RabbitMQ broker URLs
- Boost factor computation parameters
- Collection ranking configurations

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Update documentation
6. Submit a pull request

## License

This project is part of the ADS infrastructure and follows the same licensing terms.

## Contact

For questions or issues, contact the ADS development team or create an issue in the repository.
