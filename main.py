import vk_api
import re

from datetime import datetime

from config import dbconfig, login, password
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
        comments_stack = list()
        met_author_ids = set()

        for post in posts:
            p_id, p_url = post
            owner_id, post_id = extract_post_id(p_url)
            existing_comments = [c[3] for c in db.query('SELECT * FROM comments WHERE P_ID=%s', (p_id,))]
            print(f"Parsing: {p_url}")

            for c in get_all_comments(owner_id, post_id):
                if not c or c['id'] in existing_comments:
                    continue
                
                met_author_ids.add(c['from_id'])
                comments_stack.append(c)

                if len(comments_stack) >= INSERTION_COUNT:
                    met_author_ids_str = ','.join(map(str, met_author_ids))
                    existing_author_ids = set([x[0] for x in db.get_author_user_ids([met_author_ids_str,])])
                    new_author_user_ids = met_author_ids - existing_author_ids
    
                    if new_author_user_ids:
                        new_authors = get_authors_info(','.join(map(str, new_author_user_ids)))
                        db.save_authors(new_authors)

                    authors = db.get_author_ids([met_author_ids_str,])
                    for com in comments_stack:
                        try:
                            com['a_id'] = [a for a in authors if a[1] == com['from_id']][0][0]
                            com['p_id'] = p_id
                        except:
                            # Group because of negative user_id
                            # TODO Either add group table or adapt authors table for groups
                            com['a_id'] = 1
                            com['p_id'] = p_id
                            continue

                         

                    # print(comments_stack)
                    db.save_comments(comments_stack)
                    comments_stack.clear()
                    met_author_ids.clear()


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
    screen_name = jauthor.get('screen_name', " ---")
    sex = ('Ж' if jauthor['sex'] == 1 else 'М') if jauthor.get('sex') else None
    bdate = jauthor.get('bdate')
    try:
        bdate_formated = datetime.strptime(bdate, '%d.%m.%Y').strftime('%Y-%m-%d') if bdate else None
    except:
        bdate_formated = datetime.strptime(bdate, '%d.%m').strftime('0000-%m-%d') if bdate else None
    country = jauthor['country']['title'] if jauthor.get('country') else ''
    city = jauthor['city']['title'] if jauthor.get('city') else ''
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
    comment_link = f"{VK_BASE_URL}/wall{jcomment['owner_id']}_{jcomment['post_id']}?reply={jcomment['id']}"
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