import abc
import json
import logging
from logging import config
from typing import Any, Optional

from redis import Redis, exceptions
from utils.backoff import backoff

from config import LOG_CONFIG
from config import REDIS_DSL

config.dictConfig(LOG_CONFIG)


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path

    def save_state(self, state: dict) -> None:
        old_state = self.retrieve_state()
        new_state = {**old_state, **state}
        with open(self.file_path, 'w') as file:
            file.write(json.dumps(new_state))

    def retrieve_state(self) -> dict:
        try:
            with open(self.file_path, 'r') as file:
                data = file.read()
        except FileNotFoundError:
            with open(self.file_path, 'x'):
                data = None

        if not data:
            return dict()
        return json.loads(data)


class RedisStorage(BaseStorage):
    def __init__(self):
        self.db = None
        self.connect()

    @backoff()
    def connect(self):
        self.db = Redis(**REDIS_DSL)

    @backoff()
    def save_state(self, state: dict) -> None:
        try:
            self.db.mset(state)
        except Exception:
            logging.error('Ошибка подключения к базе redis')
            self.connect()
            self.db.mset(state)

    @backoff()
    def retrieve_state(self) -> dict:
        try:
            self.db.ping()
        except exceptions.ConnectionError:
            logging.error('Ошибка подключения к базе redis')
            self.connect()
        return self.db

    def __del__(self):
        if self.db:
            self.db.close()


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно
    не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    В целом ничего не мешает поменять это поведение на работу с
    БД или распределённым хранилищем.
    """

    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""

        self.storage.save_state({key: value})

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        data = self.storage.retrieve_state().get(key)
        if data:
            data = data.decode()
        return data or None
