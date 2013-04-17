import sqlite3 as sqlite

class SqliteDAO(object):
    def __init__(self, input_db):
        self._db = input_db
        self._conn = None
        self.connect()

    def __del__(self):
        self.close()

    def connect(self):
        if not self._conn:
            self._conn = sqlite.connect(self._db)

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    def get_unique_column_list(self, table, column):
        with self._conn:
            self._conn.row_factory = sqlite.Row
            cur = self._conn.cursor()

            cur.execute('SELECT DISTINCT %s FROM %s ORDER BY min(Id)' % (column, table))

        return [str(row[column]) for row in cur]

    def get_column_min_max(self, table, column):
        with self._conn:
            cur = self._conn.cursor()

            cur.execute('SELECT min(%(column)s), max(%(column)s) FROM %(table)s' % {'column':column, 'table':table})

        return cur.fetchone()

    def get_rows(self, table, column='*'):
        with self._conn:
            self._conn.row_factory = sqlite.Row
            cur = self._conn.cursor()

            cur.execute('SELECT %s FROM %s' % (column, table))

        return cur.fetchall()
