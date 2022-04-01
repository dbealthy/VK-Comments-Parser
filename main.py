import vk_api
import re

from datetime import datetime

from config import dbconfig, login, password
from db import DataBase


NOPARENT = None
INSERTION_COUNT = 50

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
        posts = db.query('SELECT * FROM posts')
        comments_stack = set()
        authors_stack = set()
        author_ids = ''

        for row in posts:
            p_id, p_url = row
            owner_id, post_id = extract_post_id(p_url)
            existing_comments = [c[2] for c in db.query('SELECT * FROM comments WHERE P_ID=%s', (p_id,))]
            print(f"Parsing: {p_url}")

            for c in get_all_comments(owner_id, post_id):
                # print(c['id'])
                if not c or c['id'] in existing_comments:
                    continue
                
                
                author_user_id = extract_id_from_url(c['from_link'])
                author_ids += author_user_id + ','

            
            
            result = db.save_authors(get_authors_info(author_ids))
            print(result)
                # a_id = db.get_author(author_user_id)

                # if not a_id:
                #     author_ids += author_user_id + ','

                # c['p_id'] = p_id
                # c['a_id'] = a_id
                # set.add(c)

                # if len(comments_stack) >= INSERTION_COUNT:
                #     authors = get_authors_info(author_ids)
                #     db.save_authors(authors)

                #     db.save_comments(list(comments_stack))
                #     comments_stack.clear()


def get_authors_info(author_ids):
    fields = "first_name, last_name, screen_name, country, city, bdate, sex, photo_max_orig"
    authors = vk.users.get(user_ids=author_ids, fields=fields)

    return [serialize_author(auth) for auth in authors]


def get_all_comments(owner_id, post_id):
    comments = tools.get_all_iter('wall.getComments', 100, {'owner_id': owner_id, 'post_id': post_id, 'thread_items_count': 10, 'need_likes': 1})
    for jcomment in comments:
        yield serialize_comment(jcomment)

        thread = jcomment['thread']

        if not thread:
            continue
    
        thread_items = thread['items']
        if thread['count'] > 10:
            threads = tools.get_all('wall.getComments', 100, {'owner_id': owner_id, 'post_id': post_id, 'comment_id': jcomment['id'], 'offset': 10, 'need_likes': 1})
            thread_items += threads.get('items')

        for jthread in thread_items:
            yield serialize_comment(jthread, parent=jcomment['id'])
            
def serialize_author(jauthor):
    link = compose_url_from_id(jauthor['id'])
    screen_name = jauthor.get('screen_name')
    sex = ('Ж' if jauthor['sex'] == 1 else 'М') if jauthor.get('sex') else None
    bdate = jauthor.get('bdate')
    try:
        bdate_formated = datetime.strptime(bdate, '%d.%m.%Y').strftime('%Y-%m-%d') if bdate else None
    except:
        bdate_formated = datetime.strptime(bdate, '%d.%m').strftime('0000-%m-%d') if bdate else None
    country = jauthor['country']['title'] if jauthor.get('country') else ''
    city = jauthor['city']['title'] if jauthor.get('title') else ''
    location = f"{country} {city}"
    photo_link = jauthor['photo_max_orig']
    return {
        'user_id': jauthor['id'], 
        'link': link, 
        'screen_name': screen_name, 
        'first_name': jauthor['first_name'], 
        'last_name': jauthor['last_name'], 
        'bdate': bdate_formated,
        'sex': sex,
        'location': location,
        'photo_link': photo_link
    }

def serialize_comment(jcomment, parent=NOPARENT):
    # Filter removed comments
    if jcomment.get('deleted') == True:
        return dict()

    timestamp = jcomment.get('date')
    date = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    from_id = jcomment.get('from_id')
    from_link = compose_url_from_id(from_id)
    comment_link = compose_comment_url_from_id(jcomment['id'])
    likes_count = jcomment.get('likes').get('count')
    parent = parent

    return {'id': jcomment['id'],
            'from_id': from_id,
            'from_link': from_link,
            'comment_link': comment_link,
            'text': jcomment['text'],
            'likes': likes_count, 
            'date': date, 
            'parent': parent}


def compose_url_from_id(id):
    url_base = 'https://vk.com/id'
    return url_base + str(id)

def compose_comment_url_from_id(id):
    # raise NotImplementedError
    pass

def extract_id_from_url(url):
    match = re.search(r'https://vk.com/id\d+', url)
    if not match:
        return ''
    id_str = match.group().replace('https://vk.com/id', '')
    return id_str              
            

def extract_post_id(url):
    match = re.search(r'wall-?\d+_\d+', url)
    if not match:
        return list()
    id_str = match.group().replace('wall', '')
    return id_str.split('_')


if __name__ == '__main__':
    main()