SQLALCHEMY_URL = ''
SQLALCHEMY_ECHO = False
API_URL = "https://api.adsabs.harvard.edu/v1" # ADS API URL
API_TOKEN = ''

# Celery configuration
CELERY_INCLUDE = ["adsboost.tasks"]
CELERY_BROKER = "pyamqp://guest:guest@rabbitmq-broker-1:5672/boost_pipeline"

OUTPUT_CELERY_BROKER = "pyamqp://guest:guest@rabbitmq-broker-1:5672/master_pipeline" 
OUTPUT_TASKNAME = "adsmp.tasks.task_update_record"

# Logging configuration
LOGGING_LEVEL = 'INFO'
LOG_STDOUT = True

# set to True adds .delay() or .apply_async() to the end of each task
# set to False for direct function calls
DELAY_MESSAGE = True

# Boost factor configuration based on RFC
# Refereed boost: 1.0 for refereed, 0.0 for non-refereed
REFEREED_BOOST_FACTOR = 1.0

# Document type boost factors (0.0 to 1.0 scale)
DOCTYPE_BOOST_FACTORS = {
    'article': 1.0,      # Journal articles (highest priority)
    'book': 0.8,          # Books
    'review': 0.6,        # Review articles
    'proceedings': 0.4,   # Conference proceedings
    'thesis': 0.2,        # Theses
    'erratum': 0.0,       # Errata (lowest priority)
    'corrigendum': 0.0    # Corrigenda (lowest priority)
}

# Document type rankings (1 = highest priority, higher numbers = lower priority)
# These will be converted to scores between 0 and 1
DOCTYPE_RANKING = {
    "article": 1,
    "eprint": 1,
    "inproceedings": 2,
    "inbook": 1,
    "abstract": 4,
    "book": 1,
    "bookreview": 4,
    "catalog": 2,
    "circular": 3,
    "erratum": 6,
    "mastersthesis": 3,
    "newsletter": 5,
    "obituary": 6,
    "phdthesis": 3,
    "pressrelease": 7,
    "proceedings": 3,
    "proposal": 4,
    "software": 2,
    "talk": 4,
    "techreport": 3,
    "misc": 8
}

# Recency boost configuration
RECENCY_BOOST_MULTIPLIER = 0.1  # Controls decay rate for reciprocal function
RECENCY_BOOST_MAX_AGE_MONTHS = 24  # Turn off boost after 24 months

# Boost combination method: 'sum', 'product', 'weighted_sum', 'weighted_geometric_mean'
BOOST_COMBINATION_METHOD = 'weighted_average'

# Boost weights for weighted combination methods
BOOST_WEIGHTS = {
    'refereed_boost': 0.4,
    'doctype_boost': 0.6,
    'recency_boost': 0.0
}


# Collection rankings for determining relative relevance weights
# Each collection defines how relevant other collections are to it using integer ranks
# Rank 1 = highest relevance, higher numbers = lower relevance
# None = no relevance (will get score of 0)

COLLECTION_RANKINGS = {
    'astrophysics': {
        'astrophysics': 1,           # Highest relevance
        'planetary': 4,
        'physics': 2,
        'heliophysics': 4,
        'general': 3,
        'earthscience': 6     # No relevance
    },
    'physics': {
        'physics': 1,             # Highest relevance
        'astrophysics': 3,
        'planetary': 3,
        'heliophysics': 3,
        'general': 2,
        'earthscience': 3     # No relevance
    },
    'earthscience': {
        'earthscience': 1,       # Highest relevance
        'planetary': 4,
        'general': 2,
        'astrophysics': 6,
        'physics': 3,
        'heliophysics': 5      # No relevance
    },
    'planetary': {
        'planetary': 1,   # Highest relevance
        'astrophysics': 5,
        'earthscience': 4,
        'physics': 2,
        'heliophysics': 2,
        'general': 3
    },
    'heliophysics': {
        'heliophysics': 1,        # Highest relevance
        'astrophysics': 6,
        'physics': 2,
        'planetary': 3,
        'general': 2,
        'earthscience': 4     # No relevance
    },
    'general': {
        'general': 1,             # Highest relevance
        'astrophysics': 1,
        'physics': 1,
        'planetary': 1,
        'earthscience': 1,
        'heliophysics': 1
    }
}

# Collection hierarchy: 'astronomy' is an umbrella that encompasses these more
# specific collections. When a record carries the umbrella AND one or more of its
# subcollections, the subcollections win and the umbrella is dropped. An umbrella
# on its own falls back to the first subcollection listed.
COLLECTION_HIERARCHY = {
    'astronomy': ['astrophysics', 'heliophysics', 'planetary']
}

# Alternate spellings seen in bib_data.database and in classifications.
# Values are compared lowercased with spaces removed, so 'Planetary Science'
# arrives here as 'planetaryscience'.
COLLECTION_ALIASES = {
    'planetaryscience': 'planetary'
}

# Citation distribution update frequency (in days)
CITATION_DISTRIBUTION_UPDATE_FREQUENCY = 90  # Quarterly updates

# Collections for citation boost calculations
COLLECTIONS = ['astrophysics', 'physics', 'earthscience', 'planetary', 'heliophysics', 'general']
