import fcntl
import json
import logging
import os
from datetime import datetime
from logging import config
from time import sleep

from config import DEFAULT_DATE, CHUNK_SIZE
from config import LOG_CONFIG, TUME_TO_RESTART
from utils.elastic_db import ELFilm
from utils.postgres_db import (PGFilmWork, transform_film,
                               transform_persons, transform_genres)
from utils.state import State, RedisStorage

config.dictConfig(LOG_CONFIG)


def run_once(fh):
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except Exception:
        logging.error("скрипт уже запущен!!!")
        os._exit(0)


def loader_es(state: State, pg: PGFilmWork, table: dict, es: ELFilm):
    table_name = table['name']
    limit = CHUNK_SIZE
    state_table = {}
    state_table_raw = state.get_state(table_name)

    if state_table_raw:
        state_table = json.loads(state_table_raw)
    date_start = state_table.get('date', DEFAULT_DATE)
    offset_start = state_table.get('offset', 0)
    date_end = date_start

    for modified_ids in pg.chunk_read_table_id(table_name, date_start, limit,
                                               offset_start):
        date_end = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        offset_start += limit
        if table.get('func_film_id', None):
            film_modified_ids = pg.get_film_id_in_table(table_name,
                                                        [item['id'] for item in
                                                         modified_ids])
        else:
            film_modified_ids = modified_ids

        transform_personal_index = table.get('transform_personal_index', None)
        if transform_personal_index:
            get_data = transform_personal_index['get_data'](
                [item['id'] for item in modified_ids]
            )
            serialize_data_index = transform_personal_index['func_transform'](
                get_data
            )
            es.set_bulk(
                transform_personal_index['index_name'],
                serialize_data_index.values()
            )


        film_result = pg.get_film_data(
            [item['id'] for item in film_modified_ids])
        if film_result:
            film_serialize = transform_film(film_result)
            es.set_bulk('movies', film_serialize.values())
        state.set_state(table['name'], json.dumps(
            {'offset': offset_start, 'date': date_start}))
    state.set_state(table['name'], json.dumps({'offset': 0, 'date': date_end}))


def process(state: State, pg: PGFilmWork, es: ELFilm) -> None:
    transform_index = {
        'persons': {
            'func_transform': transform_persons,
            'get_data': pg.get_person_data,
            'index_name': 'persons',
        },
        'genres': {
            'func_transform': transform_genres,
            'get_data': pg.get_genre_data,
            'index_name': 'genres',
        }
    }

    tables_pg = [
        {
            'name': 'genre',
            'func_film_id': True,
            'transform_personal_index': transform_index.get('genres', None)
        },
        {
            'name': 'person',
            'func_film_id': True,
            'transform_personal_index': transform_index.get('persons', None)
        },
        {
            'name': 'film_work'
        },

    ]

    for table in tables_pg:
        # да, оно с одной стороны избыточно, но могут быть ситуации когда это
        # поможет минимизировать пропуск изменяющихся данных
        logging.info('load table "{}" - start'.format(table['name']))
        loader_es(state, pg, table, es)
        logging.info('load table "{}" - success'.format(table['name']))


if __name__ == '__main__':
    fh = open(os.path.realpath(__file__), 'r')
    run_once(fh)
    state = State(RedisStorage())
    pg = PGFilmWork()
    es = ELFilm()

    while True:
        process(state, pg, es)
        sleep(TUME_TO_RESTART)
