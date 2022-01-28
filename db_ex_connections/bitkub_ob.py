from requests import Session
import requests
import asyncio
import time
import ast
import json
from admin_tools.admin_tools import connection, logger_conf, dict_values_getter
import aiohttp

# response model for ob

# {
#   "error": 0,
#   "result": [
#     [
#       1, // order id
#       1529453033, // timestamp
#       997.50, // volume [ currency ]
#       10000.00, // rate
#       0.09975000 // amount
#     ]
#   ]
# }



async def single_url_getter(session, url):
    async with session.get(url) as response:
        return await response.json()


async def multiple_ulr_getter(session, urls):
    tasks = []
    for url in urls:
        task = asyncio.create_task(single_url_getter(session, url))
        tasks.append(task)
    results = await asyncio.gather(*tasks)
    return results


async def main():
    exchange_spec_dict = json.load(open('../admin/exchanges'))
    mapped_currency = exchange_spec_dict['currency_mapping'][1]['symbols']
    rest_url = exchange_spec_dict['source'][1]['rest_url']

    url_dict = {
        'btcthb': [
            f"{rest_url}market/asks?sym={mapped_currency['btcthb']}&lmt=5",
            f"{rest_url}market/bids?sym={mapped_currency['btcthb']}&lmt=5"],
        'eththb': [
            f"{rest_url}market/asks?sym={mapped_currency['eththb']}&lmt=5",
            f"{rest_url}market/bids?sym={mapped_currency['eththb']}&lmt=5"],
        'dogethb': [
            f"{rest_url}market/asks?sym={mapped_currency['dogethb']}&lmt=5",
            f"{rest_url}market/bids?sym={mapped_currency['dogethb']}&lmt=5"],
        'manathb': [
            f"{rest_url}market/asks?sym={mapped_currency['manathb']}&lmt=5",
            f"{rest_url}market/bids?sym={mapped_currency['manathb']}&lmt=5"]
    }

    urls_list = list(dict_values_getter(url_dict))
    print(urls_list)

    while True:

        async with aiohttp.ClientSession() as session:
            response = await multiple_ulr_getter(session, urls_list)

            print(response)





asyncio.run(main())