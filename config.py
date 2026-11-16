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


# Collection rankings: how much of a record's boost_factor survives in each
# discipline's search.
#
# Outer key = the search discipline (one *_final_boost per discipline; must match
# COLLECTIONS). Inner key = a collection the record belongs to. A record in
# several collections takes the best (lowest) rank among them.
#
# Rank 1 = highest relevance. Ranks are turned into weights evenly spaced from
# 1.0 down to 0.1 across ALL the distinct rank values used anywhere in this
# table, not a fixed scale. With ranks 1-6 in use: 1=1.00, 2=0.82, 3=0.64,
# 4=0.46, 5=0.28, 6=0.10. Adding a new rank value (e.g. 7) or dropping the last
# use of one changes every step.
#
# A rank of None, or a collection a discipline does not list, gives a weight
# of 0 in that discipline's search. A 0 final boost therefore means a record's
# collections are missing from this table.
#
# 'astronomy' is ranked as its own collection: it covers records whose only
# space-science label is the 'astronomy' umbrella (see COLLECTION_HIERARCHY).
# Its ranks are currently seeded with the 'astrophysics' values, which keeps
# results identical to the old behaviour (astronomy-only -> astrophysics) until
# the project scientists' rankings are filled in.

COLLECTION_RANKINGS = {
    'astrophysics': {
        'astrophysics': 1,
        'astronomy': 1,
        'planetary': 4,
        'physics': 2,
        'heliophysics': 4,
        'general': 3,
        'earthscience': 6
    },
    'physics': {
        'physics': 1,
        'astrophysics': 3,
        'astronomy': 3,
        'planetary': 3,
        'heliophysics': 3,
        'general': 2,
        'earthscience': 3
    },
    'earthscience': {
        'earthscience': 1,
        'planetary': 4,
        'general': 2,
        'astrophysics': 6,
        'astronomy': 6,
        'physics': 3,
        'heliophysics': 5
    },
    'planetary': {
        'planetary': 1,
        'astrophysics': 5,
        'astronomy': 5,
        'earthscience': 4,
        'physics': 2,
        'heliophysics': 2,
        'general': 3
    },
    'heliophysics': {
        'heliophysics': 1,
        'astrophysics': 6,
        'astronomy': 6,
        'physics': 2,
        'planetary': 3,
        'general': 2,
        'earthscience': 4
    },
    'general': {
        'general': 1,
        'astrophysics': 1,
        'astronomy': 1,
        'physics': 1,
        'planetary': 1,
        'earthscience': 1,
        'heliophysics': 1
    }
}

# Collection hierarchy: 'astronomy' is an umbrella (the old "space science" big
# tent) that encompasses these more specific collections. When a record carries
# the umbrella AND one or more of its subcollections, the subcollections are
# more specific and win, so the umbrella is dropped. An umbrella on its own is
# kept and ranked as its own collection in COLLECTION_RANKINGS.
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

# Columns written from a boost_factors dict, in BoostFactors model-attribute order.
# Every name here is both a BoostFactors column and a compute_final_boost key.
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

# Column order for the CSV export of boost factors
EXPORT_FIELDNAMES = [
    'bibcode', 'scix_id',
    'doctype_boost', 'refereed_boost', 'recency_boost', 'boost_factor',
    'astrophysics_weight', 'physics_weight', 'earthscience_weight',
    'planetary_weight', 'heliophysics_weight', 'general_weight',
    'astrophysics_final_boost', 'physics_final_boost', 'earthscience_final_boost',
    'planetary_final_boost', 'heliophysics_final_boost', 'general_final_boost',
    'created'
]
