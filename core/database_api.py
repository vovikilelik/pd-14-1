class DatabaseApi:

    def __init__(self, cursor):
        self._cursor = cursor

    @property
    def cursor(self):
        return self._cursor

    def execute(self, query):
        print(query)  # Debug
        return self.cursor.execute(query.__str__())
