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

    def table_exists(self, table):
        return any(self.get_rows(table="sqlite_master",
                            column="count(1)",
                            where_clause="type='table' AND name='%(table)s'" % {'table': table}))

    def get_unique_column_list(self, table, column):
        with self._conn:
            self._conn.row_factory = sqlite.Row
            cur = self._conn.cursor()

            cur.execute('SELECT DISTINCT %(column)s FROM %(table)s ORDER BY Id' % {'column': column, 'table': table})

        return [str(row[column]) for row in cur]

    def get_column_min_max(self, table, column):
        with self._conn:
            cur = self._conn.cursor()

            cur.execute('SELECT min(%(column)s), max(%(column)s) FROM %(table)s' % {'column':column, 'table':table})

        return cur.fetchone()

    def get_rows(self, table, column='*', where_clause=None):
        with self._conn:
            self._conn.row_factory = sqlite.Row
            cur = self._conn.cursor()
            if where_clause:
                cur.execute('SELECT %(column)s FROM %(table)s WHERE %(where_clause)s' % {'column': column,
                                                                                         'table' : table,
                                                                                         'where_clause': where_clause})
            else:
                cur.execute('SELECT %(column)s FROM %(table)s' % {'column': column, 'table' : table})

        return cur.fetchall()
