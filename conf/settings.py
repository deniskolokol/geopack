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


# Elasticsearch
from elasticsearch import Elasticsearch
from elasticsearch_dsl.connections import connections as elastic_connections

ES_ALIAS = os.environ.get("ES_ALIAS", "default")
ES_INDEX_LOC = os.environ.get("ES_INDEX_LOC", "geoloc_v1")
ES_INDEX_ADM = os.environ.get("ES_INDEX_ADM", "geoadm_v1")
ES_SHARDS = int(os.environ.get("ES_SHARDS", 1))
ES_REPLICAS = int(os.environ.get("ES_REPLICAS", 0))
ES_HOST = os.environ.get("ES_HOST", "127.0.0.1")
ES_PORT = int(os.environ.get("ES_PORT", 9200))
ES_HTTP_AUTH = os.environ.get("ES_CREDENTIALS", "").split(":")
ES_MAIN_TIMESTAMP_FIELD = "last_updated"
ES_ADM_TIMESTAMP_FIELD = "created_at"
ES_MAX_RESULTS = 5000
ES_CONN = {
    "port": ES_PORT,
    "http_auth": ES_HTTP_AUTH,
    "timeout": 30,
    "max_retries": 10,
    "retry_on_timeout": True
    }
ES_CLIENT = Elasticsearch([ES_HOST], **ES_CONN)
elastic_connections.add_connection(alias=ES_ALIAS, conn=ES_CLIENT)
print("\n[>] Elasticsearch: {}:{}".format(ES_HOST, ES_PORT))
print("Alias: {}".format(ES_ALIAS))
print("Indices: {}, {}".format(ES_INDEX_LOC, ES_INDEX_ADM))
