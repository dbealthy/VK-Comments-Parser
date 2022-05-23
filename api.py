import asyncio
from typing import List
import aiohttp
import time

from config import tokens_list, api_v
from vk_function import VkFunction


class TokenManager:
    def __init__(self):
        self.tokens = []
        self.tokens_timeout = []
        
    def load_tokens(self, tokens):
        self.tokens = [{'token': token, 'used': 0, 'last_use': time.perf_counter()} for token in tokens]
        
        
    def get_token(self) -> str:
        # update timeout tokens
        for totoken in self.tokens_timeout:
            if time.perf_counter() - totoken['last_use'] >= 1:
                totoken['used'] = 0
                totoken['last_use'] = time.perf_counter()
                self.tokens.append(totoken)
        i = 0
        while self.tokens:
            token = self.tokens[i]
            if token['used'] == 3:
                t = self.tokens.pop(i)
                self.tokens_timeout.append(t)
                continue
            token['used'] += 1
            yield token['token']        
        else:
            yield None
        

class API:
    API_BASE = 'https://api.vk.com/method'
    
    def __init__(self, tokens: List):
        self.api_v = api_v
        self.token_manager = TokenManager()
        self.token_manager.load_tokens(tokens)
    
    def _get_token(self):
        token = next(self.token_manager.get_token())
        while not token:
            # print("Waiting for token")
            asyncio.sleep(0.5)
            token = next(self.token_manager.get_token())
        return token
        
        
    async def method(self, session, method: str, params: dict):
        token = self._get_token()
        url = f'{API.API_BASE}/{method}'
        params['access_token'] = token;  params['v'] = self.api_v
        async with session.get(url, params=params) as response:
            response = await response.json()
            return response['response']
        
        
    # async def execute(self, session, code, func_v=None):
    #     '''Access execute methods in vk that allows to run 25 api methods at once'''
    #     token = self._get_token()
            
    #     url = f'{API.API_BASE}/execute'
    #     data = {'code': code, 'access_token': token, 'v': self.api_v}
    #     async with session.post(url, data=data) as response:
    #         response = await response.json()
    #         return response['response']
    
    
    
async def get_all_iter(api, method, max_count, values=None, key='items',
                     limit=None, stop_fn=None, negative_offset=False):
        values = values.copy() if values else {}
        values['count'] = max_count

        offset = max_count if negative_offset else 0
        items_count = 0
        count = None

        while True:
            response = await vk_get_all_items(
                api, method, key, values, count, offset,
                offset_mul=-1 if negative_offset else 1
            )

            if 'execute_errors' in response:
                # raise VkToolsException(
                #     'Could not load items: {}'.format(
                #         response['execute_errors']
                #     ),
                #     response=response
                # )
                print("Execute error")
                raise Exception

            response = response['response']

            items = response["items"]
            items_count += len(items)

            for item in items:
                yield item

            if not response['more']:
                break

            if limit and items_count >= limit:
                break

            if stop_fn and stop_fn(items):
                break

            count = response['count']
            offset = response['offset']



vk_get_all_items = VkFunction(
    args=('method', 'key', 'values', 'count', 'offset', 'offset_mul'),
    clean_args=('method', 'key', 'offset', 'offset_mul'),
    return_raw=True,
    code='''
    var params = %(values)s,
        calls = 0,
        items = [],
        count = %(count)s,
        offset = %(offset)s,
        ri;
    while(calls < 25) {
        calls = calls + 1;
        params.offset = offset * %(offset_mul)s;
        var response = API.%(method)s(params),
            new_count = response.count,
            count_diff = (count == null ? 0 : new_count - count);
        if (!response) {
            return {"_error": 1};
        }
        if (count_diff < 0) {
            offset = offset + count_diff;
        } else {
            ri = response.%(key)s;
            items = items + ri.slice(count_diff);
            offset = offset + params.count + count_diff;
            if (ri.length < params.count) {
                calls = 99;
            }
        }
        count = new_count;
        if (count != null && offset >= count) {
            calls = 99;
        }
    };
    return {
        count: count,
        items: items,
        offset: offset,
        more: calls != 99
    };
''')