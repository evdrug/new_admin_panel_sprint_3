import os
from datetime import datetime
from enum import Enum

from dotenv import load_dotenv

load_dotenv()
CHUNK_SIZE = 100

LEVEL_LOG = 'INFO'

LOG_CONFIG = {
    "version": 1,
    "root": {
        "handlers": ["console"],
        "level": LEVEL_LOG
    },
    "handlers": {
        "console": {
            "formatter": "std_out",
            "class": "logging.StreamHandler",
            "level": LEVEL_LOG
        }
    },
    "formatters": {
        "std_out": {
            "format": "%(asctime)s : %(levelname)s : %(module)s : %(funcName)s : %(message)s",
            "datefmt": "%d-%m-%Y %I:%M:%S"
        }
    },
}

PG_DSL = {
    'dbname': os.environ.get('POSTGRES_DB'),
    'user': os.environ.get('POSTGRES_USER'),
    'password': os.environ.get('POSTGRES_PASSWORD'),
    'host': os.environ.get('POSTGRES_HOST'),
    'port': os.environ.get('POSTGRES_PORT'),
}

EL_DSL = {
    'hosts': ['http://{}:{}'.format(os.environ.get('ELASTIC_HOST'),
                                    os.environ.get('ELASTIC_PORT'))],
    'basic_auth': (
        os.environ.get('ELASTIC_USER'),
        os.environ.get('ELASTIC_PASSWORD')
    )
}

REDIS_DSL = {
    'host': os.environ.get('REDIS_HOST'),
    'port': os.environ.get('REDIS_PORT')
}

DEFAULT_UUID = '00000000-0000-0000-0000-000000000000'
DEFAULT_DATE = datetime(2021, 6, 13, 0, 0, 0).strftime('%Y-%m-%d %H:%M:%S')


class PersonRole(Enum):
    ACTOR = 'actor'
    WRITER = 'producer'
    DIRECTOR = 'director'


index_settings_elastic = {
    "index": "movies",
    "mappings": {
        "dynamic": "strict",
        "properties": {
            "actors": {
                "type": "nested",
                "dynamic": "strict",
                "properties": {
                    "id": {
                        "type": "keyword"
                    },
                    "name": {
                        "type": "text",
                        "analyzer": "ru_en"
                    }
                }
            },
            "actors_names": {
                "type": "text",
                "analyzer": "ru_en"
            },
            "description": {
                "type": "text",
                "analyzer": "ru_en"
            },
            "director": {
                "type": "text",
                "analyzer": "ru_en"
            },
            "genre": {
                "type": "keyword"
            },
            "id": {
                "type": "keyword"
            },
            "imdb_rating": {
                "type": "float"
            },
            "title": {
                "type": "text",
                "fields": {
                    "raw": {
                        "type": "keyword"
                    }
                },
                "analyzer": "ru_en"
            },
            "writers": {
                "type": "nested",
                "dynamic": "strict",
                "properties": {
                    "id": {
                        "type": "keyword"
                    },
                    "name": {
                        "type": "text",
                        "analyzer": "ru_en"
                    }
                }
            },
            "writers_names": {
                "type": "text",
                "analyzer": "ru_en"
            }
        }
    },
    "settings": {
        "index": {
            "refresh_interval": "1s",
            "number_of_shards": "1",
            "analysis": {
                "filter": {
                    "russian_stemmer": {
                        "type": "stemmer",
                        "language": "russian"
                    },
                    "english_stemmer": {
                        "type": "stemmer",
                        "language": "english"
                    },
                    "english_possessive_stemmer": {
                        "type": "stemmer",
                        "language": "possessive_english"
                    },
                    "russian_stop": {
                        "type": "stop",
                        "stopwords": "_russian_"
                    },
                    "english_stop": {
                        "type": "stop",
                        "stopwords": "_english_"
                    }
                },
                "analyzer": {
                    "ru_en": {
                        "filter": [
                            "lowercase",
                            "english_stop",
                            "english_stemmer",
                            "english_possessive_stemmer",
                            "russian_stop",
                            "russian_stemmer"
                        ],
                        "tokenizer": "standard"
                    }
                }
            },
            "number_of_replicas": "1",
        }
    }
}
