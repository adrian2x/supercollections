import os
import sqlite3
from collections.abc import MutableMapping
from operator import itemgetter


class SQLDict(MutableMapping):
    """
    A dictionary-like object with database persistence.
    """

    def __init__(self, dbname, table='Dict', items=[], **kw):
        self.table = table
        self.dbname = dbname
        # Create the database
        self.conn = sqlite3.connect(self.dbname)
        c = self.conn.cursor()
        c.execute(f'CREATE TABLE {table} (key text, value text)')
        c.execute(f'CREATE UNIQUE INDEX key_idx ON {table} (key)')
        self.update(items, **kw)

    def _commit(self, sql, *args):
        with self.conn as conn:
            return conn.execute(f'''
                BEGIN TRANSACTION;
                {sql}
                COMMIT;
                ''', *args)

    def __setitem__(self, key, value):
        if key in self:
            del self[key]
        self._commit(f'INSERT INTO {self.table} VALUES (?, ?)', (key, value))

    def __getitem__(self, key):
        c = self.conn.execute(f'SELECT value FROM {self.table} WHERE key=?', (key,))
        row = c.fetchone()
        if row is None:
            raise KeyError(key)
        return row[0]

    def __delitem__(self, key):
        if key in self:
            del self[key]
        self._commit(f'DELETE FROM {self.table} WHERE key=?', (key,))

    def __len__(self):
        c = self.conn.execute('SELECT COUNT(*) FROM Dict')
        row = next(c)
        return row[0]

    def __iter__(self):
        c = self.conn.execute('SELECT key FROM Dict')
        return map(itemgetter(0), c.fetchall())

    def __repr__(self):
        return f'{type(self).__name__}(dbname={self.dbname!r}, items={list(self.items())})'

    def close(self):
        self.conn.close()
