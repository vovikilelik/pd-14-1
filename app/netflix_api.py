from core.database_api import DatabaseApi
from lib.condition import Condition
from lib.select import Select

ID_FIELD_NAME = 'show_id'
MIN_ENTITY_FIELDS = (ID_FIELD_NAME, 'title')


class NetflixApi(DatabaseApi):

    def netflix_select(self, select, offset=None, sort=None, limit=None):
        query = select\
            .from_source('netflix')\
            .page(offset=offset, limit=limit)

        if sort:
            query.sort(**sort)

        return self.execute(query)

    def search_by_field(self, offset=None, limit=None, sort=None, **fields):
        not_none_list = [value for key, value in fields.items() if value]

        select = Select(*{*MIN_ENTITY_FIELDS, *fields.keys()})\
            .where(Condition(*not_none_list))

        return self.netflix_select(select, offset=offset, limit=limit, sort=sort)

    def get_by_field(self, name, values, offset=None, limit=None, sort=None, query_fields=MIN_ENTITY_FIELDS):
        rating_condition = [f"{name}='{v}'" for v in values]

        return self.search_by_field(
            rating=Condition(*rating_condition, operator='OR'),
            **dict(zip(query_fields, [None for i in query_fields])),
            offset=offset,
            limit=limit,
            sort=sort
        )

    def search_by_text(self, offset=None, limit=None, sort=None, **fields):
        field_conditions = {}
        for key, value in fields.items():
            if not value:
                field_conditions[key] = None
            elif type(value) == int:
                field_conditions[key] = f'{key}={value}'
            elif type(value) == list:
                condition = Condition(*[f"{key} LIKE '%{v}%'" for v in value], operator='OR')
                field_conditions[key] = condition
            else:
                field_conditions[key] = f"{key} LIKE '%{value}%'"

        return self.search_by_field(offset=offset, limit=limit, sort=sort, **field_conditions)

    def get_between(self, field, left, right, offset=None, limit=None, query_fields=MIN_ENTITY_FIELDS):
        select = Select(*{*query_fields, field})\
            .where(f'{field} BETWEEN {left} AND {right}')

        return self.netflix_select(select, offset=offset, limit=limit)

    def get_by_rating(self, rating, offset=None, limit=None, query_fields=MIN_ENTITY_FIELDS):
        return self.get_by_field(name='rating', values=rating, offset=offset, limit=limit, query_fields=query_fields)

    def get_by_genre(self, genre, offset=None, limit=None, query_fields=MIN_ENTITY_FIELDS):
        return self.search_by_text(
            listed_in=genre,
            **dict(zip(query_fields, [None for i in query_fields])),
            release_year=None,
            offset=offset,
            limit=limit,
            sort={'release_year': 'DESC'}
        )

    def get_by_title(self, title):
        return self.search_by_text(title=title, **{'MAX(release_year)': None})

    def get_by_id(self, show_id, *query_fields):
        select = Select(*query_fields)\
            .where(f'{ID_FIELD_NAME}={show_id}')

        return self.netflix_select(select)
