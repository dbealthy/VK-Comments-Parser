import asyncio
import aiohttp
import time

from config import tokens_list, api_v
from api import API, get_all_iter

o_id, p_id = -91050183, 25622082 # 1k comments


api = API(tokens=tokens_list)

async def main():
    async with aiohttp.ClientSession() as session:
        # tasks = []
        # for i in range(100):
        #     m = api.method(session, "wall.getComments", {'owner_id': o_id, 'post_id': p_id})
        #     tasks.append(m)
        # stime = time.perf_counter()
        # results = await asyncio.gather(*tasks, return_exceptions=True)
        # print(time.perf_counter() - stime)
        
        code = 'return API.users.get({"user_ids": 675816245});'
        result = await api.execute(session, code)
        print(result)


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
