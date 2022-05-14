from typing import List, AsyncIterator

from vkwave.api import APIOptionsRequestContext
from vkwave.vkscript import execute
import tools


@execute
def _get_all_comments_execute(api: APIOptionsRequestContext, owner_id: int, post_id: int, comment_id: int, _offset: int):
    calls = 0
    all_comments = []
    offset = _offset

    while calls < 25:
        response = api.wall.getComments(owner_id=owner_id, post_id=post_id, comment_id=comment_id, count=100, offset=offset, thread_items_count=10, need_likes=1)
        all_comments += response.items
        offset += 100
        calls += 1
    return all_comments, offset



@execute
def _get_all_split_execute(api: APIOptionsRequestContext, ids: str, sep=','):
    for id in ids.split(sep):
        pass


class Fetcher:
    @classmethod
    async def get_all_comments_iter(
        cls, api: APIOptionsRequestContext, owner_id: int, post_id: int, comment_id: int = 0) -> AsyncIterator[List[dict]]:
        offset = 0

        result = (
            await _get_all_comments_execute(
                api=api,
                owner_id=owner_id,
                post_id=post_id,
                comment_id = comment_id,
                _offset=offset,
                return_raw_response=True,
            )
        )["response"]
        executed, offset = result
        yield executed

        while executed:
            result = (
                    await _get_all_comments_execute(
                        api=api,
                        owner_id=owner_id,
                        post_id=post_id,
                        comment_id = comment_id,
                        _offset=offset,
                        return_raw_response=True,
                    )
                )["response"]
            executed, offset = result
            if executed:
                yield executed
    

    @classmethod
    async def get_authors(cls, api: APIOptionsRequestContext, ids: List[int]):
        user_fields = "first_name,last_name,screen_name,country,city,bdate,sex,photo_max_orig"
        group_fields = "name,screen_name,country,city,start_date,photo_max_orig"
        user_ids = [id for id in ids if id >= 0]
        group_ids = [id * -1 for id in ids if id < 0]

        for chunk_ids_1000 in tools.chunk(user_ids, 1000):
            yield (
                await api.users.get(
                user_ids=chunk_ids_1000,
                fields=user_fields,
                return_raw_response=True)
                )['response']

        for chunk_ids_1000 in tools.chunk(group_ids, 500):
            yield (
                await api.groups.get_by_id(
                group_ids=chunk_ids_1000, 
                fields=group_fields, 
                return_raw_response=True)
                )['response']


    @classmethod
    async def get_comments_threads(cls, api: APIOptionsRequestContext, owner_id: int, post_id: int, comments: List[dict]) -> List[dict]:
        long_comment_ids = []
        short_comment_ids = []

        for comment in comments:
            cid   = comment['id']
            count = comment['thread']['count']
            items = comment['thread']['items']


            if count <= 10:
                yield items
                
            elif count > 10:
                long_comment_ids.append(cid)

            # TODO lift this when i implement getting short comments by 25000
            # elif count <= 100:
            #     short_comment_ids.append(cid)

            
        for long_id in long_comment_ids:    
            async for chunk_2500 in Fetcher.get_all_comments_iter(api, owner_id=owner_id, post_id=post_id, comment_id=long_id):
                yield chunk_2500
                
        
        # if short_comment_ids:
        #     # split string list into chunks of 25
        #     for ids_25 in [short_threads[i:i + 25] for i in range(0, len(short_threads), 25)]:
        #         ids = ','.join(ids_25)
        #         short_items = await Fetcher.get_all_short_threads(api=api.get_context(), ids=ids)
        #         yield short_items
        
    