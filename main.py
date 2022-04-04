from datetime import datetime
from sqlite3 import Timestamp
import dateparser
from typing import Iterable, List
import vk_api
import re

from config import dbconfig, login, password
from classes import *
from db import DataBase


NOPARENT = None
INSERTION_COUNT = 50
VK_BASE_URL = "https://vk.com"

def main():
    global tools, vk
    vk_session = vk_api.VkApi(login, password)
    vk = vk_session.get_api()
    tools = vk_api.VkTools(vk_session)

    try:
        vk_session.auth(token_only=True)
    except vk_api.AuthError as error_msg:
        print(error_msg)
        return

    with DataBase(dbconfig) as db:
        posts = db.get_posts()
        for post in posts:
            p_id, p_url = post
            owner_id, post_id = extract_post_id(p_url)
            existing_comments = [c[3] for c in db.query('SELECT * FROM comments WHERE P_ID=%s', (p_id,))]
            
            print(f"Parsing: {p_url}")

            for chunk in get_all_chunked_comments(owner_id, post_id):
                # Remove empty comments and that already exist in database
                chunk = [comment for comment in chunk if comment and comment.id not in existing_comments]

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
                    met_author_ids.clear()

                # Ask primary keys of saved authors in database
                author_primary_keys = db.get_author_ids(met_author_ids_str)
                print(author_primary_keys)
                # Insert authors' primary keys to comment objects before inserting them innto database
                for comment in chunk:
                    print(comment.user_id)
                    print()
                    print([a for a in author_primary_keys if a[1] == comment.user_id])
                    comment.author_id = [a for a in author_primary_keys if a[1] == comment.user_id][0][0]
                    comment.post_id = p_id
                    # print(comment)
                   

                        
                db.save_comments(chunk)


def get_authors_info(author_ids: Iterable) -> List[Author]:
    user_ids = set() 
    group_ids = set()
    for id in author_ids:
        if id >= 0:
            user_ids.add(id)
        else:
            group_ids.add(id * -1)
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
    print(ids)
    groups = vk.groups.getById(group_ids=ids, fields=fields)
    print(groups)
    return [serialize_agroup(group) for group in groups]


def get_all_chunked_comments(owner_id: int, post_id: int, chunk_size=100) -> List[Comment]:
    comment_chunk = []
    comments = tools.get_all_iter('wall.getComments', 100, {'owner_id': owner_id, 'post_id': post_id, 'thread_items_count': 10, 'need_likes': 1})
    for jcomment in comments:
        if len(comment_chunk) >= chunk_size:
            yield comment_chunk
            comment_chunk.clear()
        else:
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
            else:
                comment_chunk.append(serialize_comment(jthread, parent_id=jcomment['id']))
    # if list has less than `chunk_size` elements yield smaller list
    if comment_chunk:
        yield comment_chunk


def serialize_agroup(jauthor: dict) -> Author:
    id = jauthor['id']*-1
    link = compose_url_from_id(id)
    name = jauthor.get('name')
    screen_name = jauthor['screen_name']
    sex = None
    bdate = parse_start_date(jauthor['start_date']) if jauthor.get('start_date') else None
    country = jauthor['country']['title'] if jauthor.get('country') else None
    city = jauthor['city']['title'] if jauthor.get('city') else None
    location = concatinate(country, city)
    photo_link = jauthor['photo_max_orig']

    return Author(id, link, screen_name, name, bdate, sex, location, photo_link)
            

def serialize_auser(jauthor: dict) -> Author:
    id = jauthor['id']
    link = compose_url_from_id(id)
    name = concatinate(jauthor['first_name'], jauthor['last_name'])
    screen_name = jauthor.get('screen_name', " ")
    sex = parse_gender(jauthor['sex'])
    bdate = parse_birthday(jauthor.get('bdate'))
    country = jauthor['country']['title'] if jauthor.get('country') else None
    city = jauthor['city']['title'] if jauthor.get('city') else None
    location = concatinate(country, city)
    photo_link = jauthor['photo_max_orig']

    return Author(id, link, screen_name, name, bdate, sex, location, photo_link)


def serialize_comment(jcomment: dict, parent_id=NOPARENT) -> Comment:
    # Filter removed comments
    if jcomment.get('deleted') == True:
        return None

    id = jcomment['id']
    text = jcomment['text']
    from_id = jcomment.get('from_id')
    author_link = compose_url_from_id(from_id)
    comment_link = f"{VK_BASE_URL}/wall{jcomment['owner_id']}_{jcomment['post_id']}?reply={jcomment['id']}"
    likes_count = jcomment.get('likes')['count']
    timestamp = str(jcomment.get('date'))
    # date = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    date = dateparser.parse(timestamp)
    
    return Comment(id, from_id, None, None, parent_id, author_link, comment_link, likes_count, text, date)


def concatinate(*args) -> str:
    return ' '.join(list(filter(lambda item: item, args)))


def parse_gender(gend):
    if not gend:
        return None

    if gend == 1:
        return 'Ж'
    elif gend == 2:
        return 'М'
    return None


def parse_birthday(bdate):
    if not bdate:
        return None
    try:
        # bdate has 01.04.2022 format 
        return datetime.strptime(bdate, '%d.%m.%Y').strftime('%Y-%m-%d')
    except:
        # bdate has 01.04 format 
        return datetime.strptime(bdate, '%d.%m').strftime('0000-%m-%d')
    

# Returns formated for database date of group creation
def parse_start_date(crdate):
    return datetime.strptime(crdate, '%Y%d%m').strftime('%Y-%m-%d')


def compose_url_from_id(id):
    url_base = 'https://vk.com/id'
    return url_base + str(id)


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