import logging
from collections import defaultdict
from logging import config
from typing import List, Dict

import psycopg2
from models import RawMovies, FilmElastick, Person
from psycopg2.extras import RealDictCursor, RealDictRow
from utils.backoff import backoff

from config import LOG_CONFIG
from config import PG_DSL
from config import PersonRole

config.dictConfig(LOG_CONFIG)


class PGConnectorBase:
    def __init__(self, logging=logging):
        self.db = None
        self.cursor = None
        self._logging = logging
        self.connect()

    @backoff(logging=logging)
    def connect(self) -> None:
        self.db = psycopg2.connect(**PG_DSL, cursor_factory=RealDictCursor)
        self.cursor = self.db.cursor()

    @backoff(logging=logging)
    def query(self, sql: str) -> List[RealDictRow]:
        try:
            self.cursor.execute(sql)
        except psycopg2.OperationalError:
            self._logging.error('Ошибка подключения к базе postgres')
            self.connect()
            self.cursor.execute(sql)
        result = self.cursor.fetchall()
        return result

    def __del__(self) -> None:
        if self.db:
            self.db.close()


class PGFilmWork(PGConnectorBase):

    def chunk_read_table_id(self, table: str, date_start: str, limit: int,
                            offset: int = 0) -> List[RealDictRow]:
        while True:
            sql_tmp = ("select id, modified "
                       "from content.{} "
                       "where modified >= %(date)s  "
                       "ORDER BY modified limit %(limit)s "
                       "offset %(offset)s").format(table)

            sql = self.cursor.mogrify(sql_tmp, {
                'date': date_start,
                'limit': limit,
                'offset': offset
            })
            table_id = self.query(sql)
            if not table_id:
                break
            yield table_id

            offset += limit
            if len(table_id) != limit:
                break

    def get_film_data(self, film_ids: List):
        sql_tmp = ("SELECT fw.id as fw_id, fw.title, fw.description, "
                   "fw.rating, fw.type, fw.created, fw.modified, "
                   "pfw.role, p.id, p.full_name, g.name "
                   "FROM content.film_work fw "
                   "LEFT JOIN content.person_film_work pfw "
                   "ON pfw.film_work_id = fw.id "
                   "LEFT JOIN content.person p "
                   "ON p.id = pfw.person_id "
                   "LEFT JOIN content.genre_film_work gfw "
                   "ON gfw.film_work_id = fw.id "
                   "LEFT JOIN content.genre g ON g.id = gfw.genre_id "
                   "WHERE fw.id IN %(films_id)s")
        sql = self.cursor.mogrify(sql_tmp, {'films_id': tuple(film_ids)})
        result = self.query(sql)
        return result

    def get_film_id_in_table(
            self,
            table: str,
            table_ids: List
    ) -> List[RealDictRow]:
        sql_tmp = ("SELECT fw.id FROM content.film_work fw "
                   "LEFT JOIN content.{table}_film_work pfw "
                   "ON pfw.film_work_id = fw.id "
                   "WHERE pfw.{table}_id IN %(ids)s "
                   "ORDER BY fw.modified").format(table=table)
        sql = self.cursor.mogrify(sql_tmp, {'ids': tuple(table_ids)})
        result = self.query(sql)
        return result

    # думаю с backoff не очень хорошая идея, застрянем в цикле и ничего не добавим
    def transform(self, films_raw: List[RealDictRow]) -> Dict:
        result = defaultdict(dict)
        for film in films_raw:
            try:
                mv = RawMovies(**film)
            except Exception as e:
                logging.error(e)
                continue
            id = mv.fw_id
            data = result[id]
            if not data:
                data = FilmElastick(**mv.dict())

            if mv.ganre_name:
                data.genre.add(mv.ganre_name)
            if mv.role:
                self.__add_role_person(mv.role, data, film)

            result[id] = data
        return result

    @staticmethod
    def __add_role_person(role, data, film):
        try:
            person = Person(**film)
        except Exception as e:
            logging.error(e)

        if role == PersonRole.ACTOR:
            if person.name not in data.actors_names:
                data.actors_names.append(person.name)
                data.actors.append(person)

        if role == PersonRole.WRITER:
            if person.name not in data.writers_names:
                data.writers_names.append(person.name)
                data.writers.append(person)

        if role == PersonRole.DIRECTOR:
            data.director.add(person.name)
