import re
from shutil import ExecError
from typing import List

import dateparser, datetime


class Parser:
    NOPARENT = None
    VK_BASE_URL = "https://vk.com"


    @classmethod
    def parse_author(cls, jauthor: dict) -> List:
        if jauthor.get('type') in ['group', 'page', 'event']:
            return Parser.parse_group(jauthor)
        return Parser.parse_user(jauthor)

    
    @classmethod
    def parse_comment(cls, jcomment: dict) -> List:
        '''If comment is deleted and doesn't have any thread then ingonre it'''
        if jcomment.get('deleted'):
            jcomment = parse_deleted_comment(jcomment)
            if not jcomment:
                return
                
        comment_id = jcomment['id']
        text = jcomment['text']
        from_id = jcomment.get('from_id', 0)
        parent_id = jcomment['parents_stack'][0] if len(jcomment['parents_stack']) else Parser.NOPARENT
        comment_link = f"{Parser.VK_BASE_URL}/wall{jcomment['owner_id']}_{jcomment['post_id']}?reply={jcomment['id']}"
        likes = jcomment.get('likes', {'count': 0}).get('count')
        timestamp = str(jcomment['date'])
        date = dateparser.parse(timestamp)
    
        return from_id, comment_id, parent_id, comment_link, text, likes, date
            


    @classmethod
    def parse_group(cls, jauthor: dict) -> List:
        group_id = jauthor['id']*-1  # Multiply by -1 because database stores groups with minuses and users as postitive int
        name = jauthor.get('name')
        screen_name = jauthor['screen_name']
        # print(group_id, screen_name, sep=' - ')
        link = compose_url_from_screen_name(screen_name)
        sex = None
        bdate = parse_bdate(jauthor['bdate']) if jauthor.get('bdate') else None
        # bdate = parse_start_date(jauthor['start_date']) if jauthor.get('start_date') else None
        country = jauthor['country']['title'] if jauthor.get('country') else None
        city = jauthor['city']['title'] if jauthor.get('city') else None
        location = concatinate(country, city)
        photo_link = jauthor['photo_max_orig']

        return group_id, link, screen_name, name, bdate, sex, location, photo_link
                
    @classmethod
    def parse_user(cls, jauthor: dict) -> List:
        user_id = jauthor['id']
        name = concatinate(jauthor['first_name'], jauthor['last_name'])
        screen_name = jauthor.get('screen_name', " ")
        # print(user_id, screen_name, sep=' - ')
        link = compose_url_from_screen_name(screen_name)
        sex = parse_gender(jauthor['sex'])
        # bdate = parse_birthday(jauthor.get('bdate'))
        bdate = parse_bdate(jauthor['bdate']) if jauthor.get('bdate') else None
        country = jauthor['country']['title'] if jauthor.get('country') else None
        city = jauthor['city']['title'] if jauthor.get('city') else None
        location = concatinate(country, city)
        photo_link = jauthor['photo_max_orig']

        return user_id, link, screen_name, name, bdate, sex, location, photo_link



def parse_deleted_comment(jcomment: dict):
    if not jcomment['thread']['count']:
        return dict()

    jcomment['owner_id'] = jcomment['thread']['items'][0]['owner_id']
    jcomment['post_id'] = jcomment['thread']['items'][0]['post_id']
    return jcomment

def concatinate(*args) -> str:
    return ' '.join(list(filter(lambda item: item, args)))

def parse_bdate(bdate):
    NOYEAR = 1904 
    # there are people whose birthday is on 29 Februar
    # and if they don't have have year in profile it raises an error
    # That is why 1904 is used, because it is a leap year.
    # Format VK bdate format: d.m.Y
    if len(bdate.split('.')) == 3:
        d, m, y = list(map(int, bdate.split('.')))
        return datetime.datetime(y, m, d)
    elif len(bdate.split('.')) == 2:
        d, m = list(map(int, bdate.split('.')))
        return datetime.datetime(NOYEAR, m, d)


def parse_gender(gend):
    genders = {1: 'Ж', 2: 'М'}
    if not gend:
        return None
    return genders[gend]


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
    return list(map(int, id_str.split('_')))
