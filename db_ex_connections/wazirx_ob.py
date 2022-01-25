#!/usr/bin/env python3

###
# TO DO:
# 1. handle with rate limit errors:
# {'code': 2136, 'message': 'Too many api request'}

# 2. responses are in list type, so we need to map it save properly to db


import asyncio
import ast
from admin_tools.admin_tools import connection, logger_conf
import time
import json
import aiohttp


async def single_url_getter(session, url):
    async with session.get(url) as response:
        message = await response.json()
        return message


async def main():
    async with aiohttp.ClientSession() as session:

        url_dict = {'btcinr': "https://api.wazirx.com/sapi/v1/depth?symbol=btcinr&limit=5",
                    'ethinr': "https://api.wazirx.com/sapi/v1/depth?symbol=ethinr&limit=5",
                    'dogeinr': "https://api.wazirx.com/sapi/v1/depth?symbol=dogeinr&limit=5",
                    'manainr': "https://api.wazirx.com/sapi/v1/depth?symbol=manainr&limit=5"}

        while True:

            tasks = []
            for k, v in url_dict.items():
                start = time.time()
                tasks.append(asyncio.ensure_future(single_url_getter(session, v)))
                end = time.time()
                await asyncio.sleep(1.5 - (end - start))  # the sleep method to avoid rate limits [ 5 api calls / sec ]

            responses = await asyncio.gather(*tasks)
            print(responses)
            # for value in responses:
            # cursor.execute(
            #     f'''INSERT INTO zonda.{k}_ob (ask_0, ask_1, ask_2, ask_3, ask_4, ask_vol_0, ask_vol_1, ask_vol_2,
            #                                                             ask_vol_3, ask_vol_4, bid_0, bid_1, bid_2, bid_3, bid_4, bid_vol_0,
            #                                                             bid_vol_1, bid_vol_2, bid_vol_3,bid_vol_4, "timestamp")
            #                                                                                     VALUES (
            #                                                                                             '{str(response_dict[k]['sell'][0]['ra'])}',
            #                                                                                             '{str(response_dict[k]['sell'][1]['ra'])}',
            #                                                                                             '{str(response_dict[k]['sell'][2]['ra'])}',
            #                                                                                             '{str(response_dict[k]['sell'][3]['ra'])}',
            #                                                                                             '{str(response_dict[k]['sell'][4]['ra'])}',
            #                                                                                             '{str(response_dict[k]['sell'][0]['ca'])}',
            #                                                                                             '{str(response_dict[k]['sell'][1]['ca'])}',
            #                                                                                             '{str(response_dict[k]['sell'][2]['ca'])}',
            #                                                                                             '{str(response_dict[k]['sell'][3]['ca'])}',
            #                                                                                             '{str(response_dict[k]['sell'][4]['ca'])}',
            #                                                                                             '{str(response_dict[k]['buy'][0]['ra'])}',
            #                                                                                             '{str(response_dict[k]['buy'][1]['ra'])}',
            #                                                                                             '{str(response_dict[k]['buy'][2]['ra'])}',
            #                                                                                             '{str(response_dict[k]['buy'][3]['ra'])}',
            #                                                                                             '{str(response_dict[k]['buy'][4]['ra'])}',
            #                                                                                             '{str(response_dict[k]['buy'][0]['ca'])}',
            #                                                                                             '{str(response_dict[k]['buy'][1]['ca'])}',
            #                                                                                             '{str(response_dict[k]['buy'][2]['ca'])}',
            #                                                                                             '{str(response_dict[k]['buy'][3]['ca'])}',
            #                                                                                             '{str(response_dict[k]['buy'][4]['ca'])}',
            #                                                                                             {int(response_dict[k]['timestamp'])}
            #                                                                                     )'''
            # )
            # time_after_db_save = time.time()
            # logger_conf().debug(f"Time of saving ob for {k}: {time_after_db_save - time_before_db_save}")


asyncio.run(main())
