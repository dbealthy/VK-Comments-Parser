import asyncio
from typing import List

from vkwave.bots import create_api_session_aiohttp
from vkwave.api.utils.get_all import Fetcher
# from vkapi_tools import Fetcher
from config import dbconfig, token
import pandas as pd


o_id = -91050183


api = create_api_session_aiohttp(token=token).api
api_ctx = api.get_context()

posts_buffer = []


def parse_post(jpost):
    id = jpost['id']
    date = jpost['date']
    comments_count = jpost['comments']['count']
    return id, comments_count, date
    


async def main():
    async for posts_chunk in Fetcher.get_all_wall_posts_iter(api_ctx, wall_owner_id=o_id):
        posts_chunk = [parse_post(post) for post in posts_chunk]
        posts_buffer.extend(posts_chunk)
        if len(posts_buffer) >= 2500:
            break 
    data = pd.DataFrame(posts_chunk).nlargest(10, 1)
    print(data)

    
        

        
        
        
  




if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())


