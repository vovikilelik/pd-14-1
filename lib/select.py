
BODY_SELECT = 'SELECT'
BODY_FROM = 'FROM'
BODY_WHERE = 'WHERE'
BODY_LIMIT = 'LIMIT'
BODY_OFFSET = 'OFFSET'
BODY_ORDER_BY = 'ORDER BY'

BODY_ORDER = [BODY_SELECT, BODY_FROM, BODY_WHERE, BODY_ORDER_BY, BODY_LIMIT, BODY_OFFSET]


class Select:

    def __init__(self, *fields):
        joined_fields = ', '.join(fields) if len(fields) > 0 else '*'
        self.body = {BODY_SELECT: joined_fields}

    def from_source(self, source):
        self.body[BODY_FROM] = source

        return self

    def where(self, condition):
        self.body[BODY_WHERE] = condition

        return self

    def page(self, limit=None, offset=None):
        if limit:
            self.body[BODY_LIMIT] = limit

        if offset:
            self.body[BODY_OFFSET] = offset

        return self

    def sort(self, **fields):
        self.body[BODY_ORDER_BY] = ', '.join([f'{f} {d}' for f, d in fields.items()])

        return self

    def get_string(self):
        query = [f'{key} {self.body[key]}' for key in BODY_ORDER if key in self.body]
        return '\n'.join(query)

    def __str__(self):
        return self.get_string()
