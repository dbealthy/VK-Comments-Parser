import re
import os
import logging
import warnings
from datetime import datetime
from typing import Iterable, List

import dateparser
import vk_api

from classes import *
from config import dbconfig, login, password
from db import VkCommentsDB

NOPARENT = None
VK_BASE_URL = 'https://vk.com'


def main() -> None:
    global tools, vk
    vk_session = vk_api.VkApi(login, password)
    vk = vk_session.get_api()
    tools = vk_api.VkTools(vk_session)

    try:
        vk_session.auth(token_only=True)
    except vk_api.AuthError as error_msg:
        logging.exception("Failed to login")
        return

    # ignore warning from dateparser library
    warnings.filterwarnings(
        "ignore",
        message="The localize method is no longer necessary, as this time zone supports the fold attribute",
    )

    with VkCommentsDB(dbconfig) as db:
        posts = db.get_posts()
        for post in posts:
            p_id, p_url = post
            owner_id, post_id = extract_post_id(p_url)
            existing_comments = [c[3] for c in db.get_comments_byid(p_id)]
            status_code = None
            post_count = 0
            print(f"Parsing: {p_url}")
           
            if exists_post(owner_id, post_id):
                chunked_comments = get_comments_bypostid(owner_id, post_id)
                status_code = Codes.Success
            else:
                chunked_comments = get_comments_bycommentid(owner_id, post_id)
                status_code = Codes.ParsedFromCommentSuccess
            try:
                for chunk in chunked_comments:
                    # Remove empty comments and that already exist in database
                    chunk = set([comment for comment in chunk if comment and comment.id not in existing_comments])

                    # Extract author ids from comments
                    met_author_ids = set(comment.user_id for comment in chunk)
                    met_author_ids_str = ','.join(map(str, met_author_ids))

                    # Get author ids that are in database 
                    existing_author_ids = set(comment[0] for comment in db.get_author_user_ids(met_author_ids_str))
                    
                    # Get author ids that are not in database yet by finding difference of two sets
                    new_author_user_ids = met_author_ids - existing_author_ids
                        
                    # Save new authors to database
                    if new_author_user_ids:
                        new_authors = get_authors_info(new_author_user_ids)
                        db.save_authors(new_authors)

                    # Ask primary keys of saved authors in database
                    author_primary_keys = db.get_author_ids(met_author_ids_str)
                    # Insert authors' primary keys to comment objects before inserting them innto database
                    for comment in chunk:
                        comment.author_id = [a for a in author_primary_keys if a[1] == comment.user_id][0][0]
                        comment.post_id = p_id           

                    db.save_comments(chunk)
                    post_count += len(chunk)
                    
            except vk_api.exceptions.ApiError:
                status_code = Codes.PostNotFoundOrDeleted
            print(status_code)
            db.save_log(PostLog(p_id, post_count, status_code.value))
            db.update_service_table(p_id, post_count)


def exists_post(owner_id, post_id) -> bool:
    try:
        vk.wall.getComments(owner_id=owner_id, post_id=post_id, count=1)
        return True
    except vk_api.exceptions.ApiError:
        return False


def get_authors_info(author_ids: Iterable) -> List[Author]:
    user_ids = [] 
    group_ids = []
    for id in author_ids:
        if id >= 0:
            user_ids.append(id)
        else:
            group_ids.append(id * -1)
    user_ids = ','.join(map(str, user_ids))
    group_ids = ','.join(map(str, group_ids))

    return get_users_info(user_ids) + get_groups_info(group_ids)

    
def get_users_info(ids: str) -> List[Author]:
    fields = "first_name,last_name,screen_name,country,city,bdate,sex,photo_max_orig"
    if not ids:
        return list()
    users = vk.users.get(user_ids=ids, fields=fields)
    return [serialize_auser(user) for user in users]


def get_groups_info(ids: str) -> List[Author]:
    fields = "name,screen_name,country,city,start_date,photo_max_orig"
    if not ids:
        return list()
    groups = vk.groups.getById(group_ids=ids, fields=fields)
    return [serialize_agroup(group) for group in groups]


def get_comments_bycommentid(owner_id, comment_id, chunk_size=100):
    comment_chunk = list()
    parrent_comment = vk.wall.getComment(owner_id=owner_id, comment_id=comment_id)['items'][0]
    comments = tools.get_all('wall.getComments', 100, {'owner_id': owner_id, 'comment_id': comment_id, 'need_likes': 1})['items']
    comment_chunk.append(serialize_comment(parrent_comment))
    for jcomment in comments:
        if len(comment_chunk) >= chunk_size: 
            yield comment_chunk
            comment_chunk.clear()
        comment_chunk.append(serialize_comment(jcomment))
    if comment_chunk:
        yield comment_chunk


def get_comments_bypostid(owner_id: int, post_id: int, chunk_size=100) -> List[Comment]:
    comment_chunk = list()
    comments = tools.get_all_iter('wall.getComments', 100, {'owner_id': owner_id, 'post_id': post_id, 'thread_items_count': 10, 'need_likes': 1})
    for jcomment in comments:
        if len(comment_chunk) >= chunk_size:
            yield comment_chunk
            comment_chunk.clear()

        comment_chunk.append(serialize_comment(jcomment))

        thread = jcomment['thread']
        if not thread:
            continue
    
        thread_items = thread['items']
        if thread['count'] > 10:
            threads = tools.get_all('wall.getComments', 100, {'owner_id': owner_id, 'post_id': post_id, 'comment_id': jcomment['id'], 'offset': 10, 'need_likes': 1})
            thread_items += threads['items']

        for jthread in thread_items:
            if len(comment_chunk) >= chunk_size:
                yield comment_chunk
                comment_chunk.clear()
            comment_chunk.append(serialize_comment(jthread, parent_id=jcomment['id']))
    # if list has less than `chunk_size` elements yield smaller list
    if comment_chunk:
        yield comment_chunk


def serialize_agroup(jauthor: dict) -> Author:
    id = jauthor['id']*-1  # Multiply by -1 because database stores groups with minuses and users as postitive int
    name = jauthor.get('name')
    screen_name = jauthor['screen_name']
    link = compose_url_from_screen_name(screen_name)
    sex = None
    bdate = dateparser.parse(jauthor['bdate']) if jauthor.get('bdate') else None
    # bdate = parse_start_date(jauthor['start_date']) if jauthor.get('start_date') else None
    country = jauthor['country']['title'] if jauthor.get('country') else None
    city = jauthor['city']['title'] if jauthor.get('city') else None
    location = concatinate(country, city)
    photo_link = jauthor['photo_max_orig']

    return Author(id, link, screen_name, name, bdate, sex, location, photo_link)
            

def serialize_auser(jauthor: dict) -> Author:
    id = jauthor['id']
    name = concatinate(jauthor['first_name'], jauthor['last_name'])
    screen_name = jauthor.get('screen_name', " ")
    link = compose_url_from_screen_name(screen_name)
    sex = parse_gender(jauthor['sex'])
    # bdate = parse_birthday(jauthor.get('bdate'))
    bdate = dateparser.parse(jauthor['bdate']) if jauthor.get('bdate') else None
    country = jauthor['country']['title'] if jauthor.get('country') else None
    city = jauthor['city']['title'] if jauthor.get('city') else None
    location = concatinate(country, city)
    photo_link = jauthor['photo_max_orig']

    return Author(id, link, screen_name, name, bdate, sex, location, photo_link)


def serialize_comment(jcomment: dict, parent_id=NOPARENT) -> Comment:
    # Filter removed comments
    if jcomment.get('deleted'):
        return None

    id = jcomment['id']
    text = jcomment['text']
    from_id = jcomment.get('from_id')
    author_link = compose_url_from_id(from_id)
    comment_link = f"{VK_BASE_URL}/wall{jcomment['owner_id']}_{jcomment['post_id']}?reply={jcomment['id']}"
    likes_count = jcomment.get('likes')['count']
    timestamp = str(jcomment.get('date'))
    date = dateparser.parse(timestamp)
    
    return Comment(id, from_id, None, None, parent_id, author_link, comment_link, likes_count, text, date)


def concatinate(*args) -> str:
    return ' '.join(list(filter(lambda item: item, args)))


def parse_gender(gend):
    if not gend:
        return None

    if gend == 1:
        return '??'
    elif gend == 2:
        return '??'
    return None


def compose_url_from_id(id):
    url_base = 'https://vk.com/id'
    return url_base + str(id)

def compose_url_from_screen_name(screen_name):
    url_base = 'https://vk.com/'
    return url_base + str(screen_name)


def compose_comment_url_from_id(base_url, id):
    return f"{base_url}_r{id}"      


def extract_post_id(url):
    match = re.search(r'wall-?\d+_\d+', url)
    if not match:
        return list()
    id_str = match.group().replace('wall', '')
    return id_str.split('_')


if __name__ == '__main__':
    main()

    
