"""GeoPack project settings."""

import os
from logging import config


# Project base directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# Relative paths inside project directory
def rel(*x):
    return os.path.join(BASE_DIR, *x)


# Project-wide
ENCODING = 'UTF-8'
PROJECT_NAME = 'GeoPack'
PROJECT_MOTTO = 'free text geo-parsing and geo-tagging'
SSL_ENABLED = bool(int(os.environ.get('SSL_ENABLED', 0)))
GEOPACK_SERVER_DOMAIN = os.environ.get('GEOPACK_SERVER_DOMAIN', '')
GEOPACK_SERVER_PORT = int(os.environ.get('GEOPACK_SERVER_PORT', 80))

PROJECT_DOMAIN = f"{'https' if SSL_ENABLED else 'http'}://{GEOPACK_SERVER_DOMAIN}"
PROJECT_DOMAIN += '/' if GEOPACK_SERVER_PORT in [443, 80] else f":{GEOPACK_SERVER_PORT}/"

# Logging (pure console).
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'default': {
            'format': '[%(asctime)s %(levelname)s] %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S %z',
        }
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'default',
        },
    },
    'loggers': {
        'root': {
            'handlers': ['console'],
            'level': 'DEBUG',
            'propagate': True,
        }
    }
}
config.dictConfig(LOGGING)


# API schema.
__TOKEN = {
    'type': 'str',
    'required': True,
    'help_text': 'Authorization token.'
    }
__LANG = {
    'type': 'str',
    'required': True,
    'help_text': 'If omitted, the back-end will try to identify the language.'
}
API_SCHEMA = {
    'version': os.environ.get('API_VERSION', '0.0.1'),
    'endpoints': {
        'geotag': {
            'methods': ['GET', 'POST'],
            'params': {
                'text': {
                    'type': 'str',
                    'required': True,
                    'help_text': 'Free-form text'
                },
                'lang': __LANG,
                'token': __TOKEN
            }
        },
        'geoplace': {
            'methods': ['GET', 'POST'],
            'params': {
                'query': {
                    'type': 'str',
                    'required': True,
                    'help_text': 'Place name (address, town/city, neighborhood, etc.)'
                },
                'lang': __LANG,
                'token': __TOKEN
            }
        }
    }
}


# Communicaton.
GEOPACK_SERVER_PORT = int(os.environ.get('GEOPACK_SERVER_PORT', 80))


# Authentication.
GEOPACK_TOKEN = os.environ.get('GEOPACK_TOKEN', '')


# Default language.
LANG_DEFAULT = 'en'


# NLP features.
LANG_USING_UDPIPE = ['sk', 'cs', 'fi', 'lv']

# Model names for languages; for languages using spacy_udpipe - it is the same e.g. 'sk':'sk'
SPACY_LANG_MODEL = {
    'en': 'en_core_web_sm',
    'sk': 'sk',
    'cs': 'cs',
    'pl' : 'pl_spacy_model',
    'el' : 'el_core_news_sm',
    'fi' : 'fi',
    'lv' : 'lv',
    'nl' : 'nl_core_news_sm',
    'it' : 'it_core_news_sm',
    'es' : 'es_core_news_sm'
}

# Alternative NER models - for languages which don't have spaCy
# "all-in-one" model and use spacy_udpipe can be a specific spaCy
# NER model (e.g. "xx_ent_wiki_sm") or "polyglot" to use polyglot's
# Named Entity Recognition.
SPACY_NER_LANG_MODEL = {
    'sk': 'polyglot',
    'cs': 'polyglot',
    'fi': 'polyglot',
    'lv': 'polyglot'
}
