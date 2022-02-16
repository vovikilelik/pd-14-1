import sqlite3

from app.netflix_api import NetflixApi, MIN_ENTITY_FIELDS, ID_FIELD_NAME
from lib.condition import Condition

FULL_ENTITY_FIELDS = list({*MIN_ENTITY_FIELDS, 'country', 'release_year', 'listed_in', 'description'})
PREVIEW_ENTITY_FIELDS = list({*MIN_ENTITY_FIELDS, 'description'})

ROW_MAP_PAIR = {
    'show_id': 'show_id',
    'title': 'title',
    'country': 'country',
    'release_year': 'release_year',
    'listed_in': 'genre',
    'description': 'description',
    'rating': 'rating'
}


def map_row(raw_row):
    result = {}

    for key in raw_row.keys():
        pair = ROW_MAP_PAIR.get(key)
        if pair:
            result[pair] = raw_row[key]

    return result


class NetflixWorker:
    """
    Агрегатор запросов к БД netflix
    """

    def __init__(self, _database_path):
        self._database_path = _database_path

    @property
    def database_path(self):
        return self._database_path

    def __enter__(self):
        try:
            self._connection = sqlite3.connect('./res/netflix.db')
        except Exception:
            raise ConnectionError('Не удалось подключиться к базе данных')
        else:
            self._connection.row_factory = sqlite3.Row
            self._api = NetflixApi(self._connection.cursor())

            return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._connection.close()

    def get_by_id(self, show_id):
        """
        Возвращает полную запись по указанному show_id
        """
        response = self._api.get_by_id(f"'{show_id}'", *FULL_ENTITY_FIELDS)
        row = response.fetchone()

        return map_row(row)

    def get_by_title(self, title_text):
        """
        Возвращает самую свежую, с точностью до года, полную запись
        """
        response = self._api.get_by_title(title=title_text)
        row = response.fetchone()

        return self.get_by_id(row[ID_FIELD_NAME])

    def get_between_list(self, field, left, right, offset=None, limit=None):
        """
        Возвращает сокращённый список между двумя датами
        """
        response = self._api.get_between(
            field,
            left,
            right,
            offset=offset,
            limit=limit,
            query_fields=PREVIEW_ENTITY_FIELDS
        )

        return [map_row(row) for row in response.fetchall()]

    def get_rating_list(self, rating, offset=None, limit=None):
        """
        Возвращает сокращённый список по рейтингу
        """
        response = self._api.get_by_rating(
            rating,
            offset=offset,
            limit=limit,
            query_fields=PREVIEW_ENTITY_FIELDS
        )

        return [map_row(row) for row in response.fetchall()]

    def get_genre_list(self, genre, offset=None, limit=None):
        """
        Возвращает сокращённый список по жанру
        """
        response = self._api.get_by_genre(
            genre,
            offset=offset,
            limit=limit,
            query_fields=PREVIEW_ENTITY_FIELDS
        )

        return [map_row(row) for row in response.fetchall()]

    def get_actor_pair_list(self, origin, friend):
        """
        Возвращает список актёров, которые были в паре с переданными в аргументах, больше 2-х раз
        """
        response = self._api.search_by_text(
            **{'"cast"': [origin, friend]}
        )
        cast_times = [row['cast'] for row in response.fetchall()]

        times_count = {}

        for time in cast_times:
            actors = list(map(lambda w: w.strip(), time.split(',')))

            for a in actors:
                if a in times_count:
                    times_count[a] += 1
                else:
                    times_count[a] = 1

        return [actor for actor, count in times_count.items() if count > 2]

    def search(self, movie_type, release_year, genre):
        """
        Совершает поиск по типу, году и жанру.
        Возвращает сокращённый список.
        """
        response = self._api.search_by_text(
            type=movie_type,
            release_year=release_year,
            listed_in=genre,
            description=None
        )

        return [map_row(row) for row in response.fetchall()]


