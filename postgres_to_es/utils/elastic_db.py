import logging
from logging import config

import elasticsearch
from elasticsearch import Elasticsearch, helpers
from pydantic.main import BaseModel
from utils.backoff import backoff

from config import EL_DSL
from config import LOG_CONFIG
from config import elastic_index

config.dictConfig(LOG_CONFIG)


class ELConnectorBase:
    def __init__(self):
        self.connect()

    @backoff(logging=logging)
    def connect(self):
        self.client = Elasticsearch(**EL_DSL)
        self.__check_and_create_index()

    def __check_and_create_index(self):
        for name, index_setting in elastic_index.items():
            if not self.client.indices.exists(
                    index=name
            ):
                self.client.indices.create(
                    **index_setting,
                    ignore=400
                )

    def __del__(self):
        if self.client:
            self.client.close()


class ELFilm(ELConnectorBase):

    @backoff(logging=logging)
    def set_bulk(self, index, data):
        try:
            helpers.bulk(self.client, self.generate_elastic_data(index, data))
        except elasticsearch.exceptions.ConnectionError:
            logging.error('Ошибка подключения к базе elasticsearch')
            self.connect()
            helpers.bulk(self.client, self.generate_elastic_data(index, data))

    def generate_elastic_data(self, index, data: list[BaseModel]):
        for item in data:
            yield {
                '_index': index,
                '_id': item.id,
                '_source': item.json()
            }
