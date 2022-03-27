from datetime import datetime
import requests
import datetime
import re
from typing import NamedTuple

from config import access_token, api_v, dbconfig
from db import DataBase

class Comment(NamedTuple):
    p_id: int
    text: str
    author_link: str
    author_name: str
    p_date: datetime.datetime


def main():
    with DataBase(dbconfig) as db:
        posts = db.query("SELECT * FROM posts")
        for post in posts:
            p_id, p_url = post
            owner_id, post_id = extract_post_id(p_url)
            print(owner_id, post_id)
            comments = get_comments(owner_id, post_id)
            print(comments)
            # db.save_comments(comments)

        
def extract_post_id(url):
    match = re.search(r"wall-?\d+_\d+", url)
    if not match:
        return list()
    id_str = match.group().replace("wall", "")
    return id_str.split('_')


def ask_api(method: str, params: dict):
    base_url = "https://api.vk.com/method"
    params_str = '&'.join([f"{key}={value}" for key, value in params.items()])
    url = f"{base_url}/{method}?{params_str}&access_token={access_token}&v={api_v}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response
    except Exception as e:
        print(e)


def get_comments(owner_id, post_id, count=10):
    method = "wall.getComments"
    params = {
        "owner_id": owner_id,
        "post_id": post_id,
        "count": count,
        "extended": 1,
        "need_likes": 0}

    response = ask_api(method, params)
    return response.json()
   


if __name__ == "__main__":
    main()
