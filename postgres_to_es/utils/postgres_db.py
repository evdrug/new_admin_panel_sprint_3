import logging
from collections import defaultdict
from logging import config
from typing import List, Dict

import psycopg2
from psycopg2.extras import RealDictCursor, RealDictRow

from config import LOG_CONFIG
from config import PG_DSL
from config import PersonRole
from models import (RawMovies, FilmElastick, Person, PersonElastic,
                    PersonRaw, Genre, GenreRaw, GenreElastic)
from utils.backoff import backoff

config.dictConfig(LOG_CONFIG)


def add_role_person(role, data, film):
    mapping_person = {
        PersonRole.ACTOR.value: {
            'names': data.actors_names,
            'obj': data.actors
        },
        PersonRole.WRITER.value: {
            'names': data.writers_names,
            'obj': data.writers
        },
        PersonRole.DIRECTOR.value: {
            'names': data.directors_names,
            'obj': data.directors
        },
    }
    try:
        person = Person(**film)
    except Exception as e:
        logging.error(e)
    data_mapping = mapping_person.get(role, None)
    if data_mapping:
        data_mapping['names'].append(person.name)
        data_mapping['obj'].append(person)


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

    def get_person_data(self, ids: List) -> List[RealDictRow]:
        sql_tmp = (
            "select p.id, full_name, pfw.role, pfw.film_work_id "
            "from content.person p "
            "left join content.person_film_work pfw on p.id = pfw.person_id "
            "WHERE p.id IN %(persons_ids)s"
        )

        sql = self.cursor.mogrify(sql_tmp, {
            'persons_ids': tuple(ids)
        })
        result = self.query(sql)
        return result

    def get_genre_data(self, ids: List) -> List[RealDictRow]:
        sql_tmp = (
            "select g.id, g.name, g.description, gfw.film_work_id "
            "from content.genre g "
            "join content.genre_film_work gfw on g.id = gfw.genre_id "
            "WHERE g.id IN %(genres_ids)s"
        )

        sql = self.cursor.mogrify(sql_tmp, {
            'genres_ids': tuple(ids)
        })
        result = self.query(sql)
        return result

    def get_film_data(self, film_ids: List):
        if not film_ids:
            return None
        sql_tmp = ("SELECT fw.id as fw_id, fw.title, fw.description, "
                   "fw.rating, fw.type, fw.created, fw.modified, "
                   "pfw.role, p.id, p.full_name, g.name , g.id as genre_id "
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


def transform_film(films_raw: List[RealDictRow]) -> Dict:
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

        genre_name = mv.genre_name
        if genre_name and genre_name not in data.genres_names:
            genre = Genre(id=mv.genre_id, name=genre_name)
            data.genres_names.append(genre_name)
            data.genres.append(genre)

        if mv.role:
            add_role_person(mv.role, data, film)

        result[id] = data
    return result


def transform_persons(
        get_data: List[RealDictRow],
) -> List:
    result = defaultdict(dict)
    for person in get_data:
        try:
            p_raw = PersonRaw(**person)
        except Exception as e:
            logging.error(e)
            continue
        id = p_raw.id
        data = result[id]
        if not data:
            data = PersonElastic(**p_raw.dict())
        data.role.add(p_raw.role_raw)
        data.film_ids.add(p_raw.film_work_id)
        result[id] = data
    return result


def transform_genres(
        get_data: List[RealDictRow],
) -> List:
    result = defaultdict(dict)
    for genre in get_data:
        try:
            genre_raw = GenreRaw(**genre)
        except Exception as e:
            logging.error(e)
            continue
        id = genre_raw.id
        data = result[id]
        if not data:
            data = GenreElastic(**genre_raw.dict())
        data.description = genre_raw.description
        data.film_ids.add(genre_raw.film_work_id)
        result[id] = data
    return result
