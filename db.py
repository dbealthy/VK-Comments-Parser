from xxlimited import Str
import mysql.connector
from typing import List
from classes import *


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

    def get_posts(self) -> List[List]:
        return self.query('SELECT * FROM posts')

    def get_comments_byid(self, p_id):
        return self.query('SELECT * FROM comments WHERE P_ID=%s', (p_id,))


    def save_comments(self, comments: List[Comment]) -> None:
        params = [comment.values() for comment in comments]

        sql_insert = "INSERT INTO `comments` (P_ID, A_ID, comment_id, author_link, comment_link, comment_text, likes, comment_date, parent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        self._cursor.executemany(sql_insert, params)
        self.commit()
    

    def save_authors(self, authors: List[Author]) -> None:
        params =  [auth.values() for auth in authors]
                    
        sql_insert = "INSERT INTO `authors` (user_id, link, screen_name, name, bdate, sex, location, photo_link) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        ids = self._cursor.executemany(sql_insert, params)
        self.commit()


    def get_author(self, author_id: int) -> List:
        return self.quiery("SELECT AUTHOR_ID FROM authors WHERE user_id=%s", author_id)

    def get_author_ids(self, author_ids: str) -> List[List]:
        if not author_ids:
            return list()
        return self.query('SELECT `AUTHOR_ID`, `user_id` FROM `authors` WHERE LOCATE(user_id, %s) > 0', (author_ids,))

    def get_author_user_ids(self, user_ids: str) -> List[List]:
        if not user_ids:
            return list()
        return self.query('SELECT `user_id` FROM `authors` WHERE LOCATE(user_id, %s) > 0', (user_ids,))