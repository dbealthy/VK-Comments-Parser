import mysql.connector


class DataBase:
    def __init__(self, config):
        self._conn = mysql.connector.connect(**config)
        self._cursor = self._conn.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def connection(self):
        return self._conn

    @property
    def cursor(self):
        return self._cursor

    def commit(self):
        self.connection.commit()

    def close(self, commit=True):
        if commit:
            self.commit()
        self.connection.close()

    def execute(self, sql, params=None):
        self.cursor.execute(sql, params or ())

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()

    def query(self, sql, params=None):
        self.cursor.execute(sql, params or ())
        return self.fetchall()


    # def save_items(self, items):
    #     if items:
    #         params = [tuple(item.values()) for item in items]
    #         sql_insert = "INSERT IGNORE INTO `items` (res_id, link, title, content, nd_date, s_date, not_date) VALUES (%s, %s, %s, %s, %s, UNIX_TIMESTAMP(CURRENT_TIMESTAMP), %s)"
    #         self._cursor.executemany(sql_insert, params)
    #         self.commit()

    def save_comments(self, comments):
        pass
        