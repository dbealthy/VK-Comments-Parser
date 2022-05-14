import asyncio
import time
from typing import List

from vkwave.bots import create_api_session_aiohttp

from vkapi_tools import Fetcher
from database import VkCommentsDB
from config import dbconfig, token

from parse import Parser, extract_post_id


api = create_api_session_aiohttp(token=token).api
api_ctx = api.get_context()

# o_id, p_id = -91050183, 25622082  # Success (1k)
# o_id, p_id = -91050183, 25586211  # Success (5.4k)
# o_id, p_id = -91050183, 1032715   # Fail    (13k)
# o_id, p_id = -91050183, 25554036  # Success (40k)
# o_id, p_id = -91050183, 25427474  # Success (54k) 100mb ram max

# IDEA: somehow organize diverse comment chunks into sized chunks of 25k 
# When chunk is fully composed its auther_ids are gonna be extracted
# Return 25000 comments, extract author_ids, make request to get data from ids
# Save authors, save comments, 


# db_p_id = 11

async def main():
    disable_dataparser_warnings()
    total_comments = 0
    total_authors = 0

    with VkCommentsDB(dbconfig) as db:
        for post in db.get_posts():
            db_p_id, url = post
            o_id, p_id = extract_post_id(url)
            print(f"Parsing: {url}", end='')
            start_time = time.perf_counter()
            async for chunk in get_schunked_comments(api_ctx, o_id, p_id):
                existing_author_ids = {a_id[0] for a_id in db.get_author_ids()}
                author_ids = {comment[0] for comment in chunk}
                authors_ids = author_ids - existing_author_ids
                authors = [Parser.parse_author(author) async for author_chunk in Fetcher.get_authors(api_ctx, authors_ids) for author in author_chunk]
                db.save_authors(authors)
                db.save_comments(db_p_id, chunk)
                total_authors += len(authors)
                total_comments += len(chunk)

            print(time.perf_counter() - start_time)
            print(f"Comments: {total_comments}")
            print(f"Authors:  {total_authors}")



async def get_schunked_comments(api, owner_id, post_id, chunk_size=25000):
    result = []
    async for chunk in get_all_comments(api, owner_id, post_id):
        for comment in chunk:
            comment = Parser.parse_comment(comment)
            if len(result) == chunk_size:
                yield result
                result.clear()

            result.append(comment)
    if result:
        yield result


async def get_all_comments(api, owner_id, post_id):
    '''Gets all comments and thread comments from a post '''
    async for chunk_2500 in Fetcher.get_all_comments_iter(api, owner_id=owner_id, post_id=post_id):
        yield chunk_2500
        async for cthr_chunk in Fetcher.get_comments_threads(api, owner_id, post_id, chunk_2500):
            yield cthr_chunk

def disable_dataparser_warnings():
    import warnings
    warnings.filterwarnings(
        "ignore",
        message="The localize method is no longer necessary, as this time zone supports the fold attribute",
    )

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())


