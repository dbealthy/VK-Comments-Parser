import asyncio
import time
from typing import List

import vkwave
from vkwave.bots import create_api_session_aiohttp

from config import dbconfig, token
from database import VkCommentsDB
from parse import Parser, extract_post_id
from tools import StatusCodes
from vkapi_tools import Fetcher

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


async def main():
    disable_dataparser_warnings()
    task = get_task()
    db_p_id, url = task

    while task:
        o_id, p_id = extract_post_id(url)
        statuscode = await check_post(api_ctx, o_id, p_id)
        print(f"Parsing: [{db_p_id}] {url}", end="")
        print('Status: ', statuscode, end=' | ')
        match statuscode:
            case StatusCodes.ParsedFromPost:
                await handle_post(api_ctx, db_p_id, o_id, p_id)
            
            case StatusCodes.ParsedFromCommentSuccess:
                await handle_comment(api_ctx, db_p_id, o_id, p_id)
            case _:
                pass
        with VkCommentsDB(dbconfig) as db:
            db.update_task(db_p_id, statuscode)
        task = get_task()
                
                
def get_task():
    with VkCommentsDB(dbconfig) as db:
        task = db.get_task()
        db_p_id, url = task
        db.update_task(db_p_id, StatusCodes.InProcess)     
        return task
        
        
async def handle_comment(api, db_pid, o_id, c_id):
    total_comments = 0
    total_authors = 0
    start_time = time.perf_counter()
    async for chunk in Fetcher.get_all_comments_iter(api, owner_id=o_id, post_id=0, comment_id=c_id):
        chunk = [Parser.parse_comment(comment) for comment in chunk]
        cc, ac = await handle_comments(chunk, api, db_pid)
        total_comments += cc; total_authors += ac

    print(round(time.perf_counter() - start_time, 3), end=' | ')
    print(f"Comments: {total_comments}", end=' | ')
    print(f"Authors:  {total_authors}")
    print()



async def handle_post(api, db_pid, o_id, p_id):
    total_comments = 0
    total_authors = 0
    start_time = time.perf_counter()
    async for chunk in get_schunked_comments(api_ctx, o_id, p_id):
        cc, ac = await handle_comments(chunk, api, db_pid)
        total_comments += cc; total_authors += ac

    print(round(time.perf_counter() - start_time, 3), end=' | ')
    print(f"Comments: {total_comments}", end=' | ')
    print(f"Authors:  {total_authors}")
    print()
    

async def handle_comments(comments, api, db_pid):
    with VkCommentsDB(dbconfig) as db:
        existing_author_ids = {a_id[0] for a_id in db.get_author_ids()}
        author_ids = {comment[0] for comment in comments}
        authors_ids = author_ids - existing_author_ids
        authors = [Parser.parse_author(author) async for author_chunk in Fetcher.get_authors(api, authors_ids) for author in author_chunk]
        db.save_authors(authors)
        db.save_comments(db_pid, comments)
    return len(comments), len(authors)

        
        

async def check_post(api, o_id, p_id):
    try:
        try:
            await api.wall.get_comments(owner_id=o_id, post_id=p_id, count=0, return_raw_response=True)
            return StatusCodes.ParsedFromPost
        except vkwave.api.methods._error.APIError as e:
            
            if e.code == 212:
                return StatusCodes.AccessToPostCommentsDenied
            elif e.code == 15:
                await api.wall.get_comments(owner_id=o_id, comment_id=p_id, count=0, return_raw_response=True)
                return StatusCodes.ParsedFromCommentSuccess
            else:
                print('Unexpected error code')
                print(e)
                return StatusCodes.NotFoundOrDeleted
            
    except Exception as e:
        # print(e)
        return StatusCodes.NotFoundOrDeleted


async def get_schunked_comments(api, owner_id, post_id, chunk_size=25000):
    result = []
    async for chunk in get_all_comments(api, owner_id, post_id):
        for comment in chunk:
            comment = Parser.parse_comment(comment)
            if len(result) == chunk_size:
                yield result
                result.clear()
            
            if not comment:
                continue

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


