import datetime
from typing import Optional, List, Set

from pydantic import BaseModel
from pydantic.fields import Field


class Person(BaseModel):
    id: str
    name: str = Field(alias='full_name')


class FilmElastick(BaseModel):
    id: str = Field(alias='fw_id')
    title: str
    description: Optional[str]
    imdb_rating: Optional[float] = Field(alias='rating', default=0)
    actors: List[Person] = []
    actors_names: List = []
    writers: List[Person] = []
    writers_names: List = []
    director: Set = set()
    genre: Set = set()


class RawMovies(BaseModel):
    fw_id: str
    title: str
    description: Optional[str]
    rating: Optional[float]
    type: str
    created: datetime.datetime
    modified: datetime.datetime
    role: Optional[str]
    person_id: Optional[str] = Field(alias='id')
    person_name: Optional[str] = Field(alias='full_name')
    ganre_name: Optional[str] = Field(alias='name')
