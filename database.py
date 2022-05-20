import mysql.connector
from typing import List


class MySqlDataBase:
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


class VkCommentsDB(MySqlDataBase):
    def get_task(self) -> List[List]:
        
        return self.query('''
           SELECT posts.POST_ID, posts.POST_URL FROM `tasks` 
           INNER JOIN `posts` ON tasks.p_id=posts.POST_ID 
           WHERE tasks.untill > NOW() AND (DATEDIFF(NOW(), tasks.last_update) >= 1 OR tasks.last_update IS NULL) AND tasks.status != 22 AND tasks.status != 20
           ORDER BY tasks.last_update
           LIMIT 1; 
                          ''')[0]


    def get_comments_byid(self, p_id):
        return self.query('SELECT * FROM comments WHERE P_ID=%s', (p_id,))


    def save_comments(self, p_id, comments: List[List]) -> None:
        p_id = (p_id,)
        params = [p_id + comment for comment in comments]

        sql_insert = "INSERT INTO `comments` (P_ID, A_ID, comment_id, parent_id, comment_link, comment_text, likes, comment_date, adate) \
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW()) ON DUPLICATE KEY UPDATE likes=VALUES(likes)"
       
        self._cursor.executemany(sql_insert, params)
        self.commit()
      
        

    def save_authors(self, authors: List[List]) -> None:
                   
        sql_insert = "INSERT INTO `authors` (AUTHOR_ID, link, screen_name, name, bdate, sex, location, photo_link, adate) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())"
        self._cursor.executemany(sql_insert, authors)
        self.commit()


    def get_author(self, author_id: int) -> List:
        return self.query("SELECT AUTHOR_ID FROM authors WHERE user_id=%s", author_id)


    def get_author_ids(self) -> List[List]:
        return self.query('SELECT `AUTHOR_ID` FROM `authors`')


    def get_author_user_ids(self, user_ids: str) -> List[List]:
        return self.query('SELECT `user_id` FROM `authors` WHERE LOCATE(user_id, %s) > 0', (user_ids,)) if user_ids else list()


    def update_task(self, db_pid, status):
        
        self.execute('UPDATE tasks SET status=%s WHERE p_id=%s', (status.value, db_pid))
        self.execute('UPDATE tasks SET last_update=NOW() WHERE p_id=%s', (db_pid, ))






