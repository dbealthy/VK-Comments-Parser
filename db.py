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


    def save_comments(self, comments: list):
        # params = [(com["P_ID"], com["comment_id"], com["author_name"], com["comment_text"], com["comment_date"], com["replies_to_id"]) for com in comments]
        sql_insert = "INSERT INTO `comments` (P_ID, comment_id, author_link, comment_text, comment_date, parent) VALUES (%s, %s, %s, %s, %s, %s)"
        self._cursor.executemany(sql_insert, comments)
        self.commit()

    def save_authors(self, authors):
        params =  [(auth['user_id'], 
                    auth['link'], 
                    auth['screen_name'], 
                    auth['first_name'], 
                    auth['last_name'], 
                    auth['bdate'],
                    auth['sex'],
                    auth['location'],
                    auth['photo_link'])for auth in authors]
                    
        sql_insert = "INSERT INTO `authors` (user_id, link, screen_name, first_name, last_name, bdate, sex, location, photo_link) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        ids = self._cursor.executemany(sql_insert, params)
        self.commit()
        return ids

    def get_author(self, author_id):
        return self.quiery("SELECT AUTHOR_ID FROM authors WHERE user_id=%s", author_id)
