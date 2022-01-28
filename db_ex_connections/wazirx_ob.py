#!/usr/bin/env python3

###
# TO DO:
# 1. handle with rate limit errors:
# {'code': 2136, 'message': 'Too many api request'}

# 2. responses are in list type, so we need to map it save properly to db

import sys
sys.path.append("C:/Users/oskar/Desktop/hft_algo/hft_algo")

import asyncio
from admin.admin_tools import connection, logger_conf
import time
import json
import aiohttp


async def single_url_getter(session, url):
    async with session.get(url) as response:
        message = await response.json()
        return message


async def main():

    cursor = connection()
    logger = logger_conf("../db_ex_connections/wazirx.log")
    async with aiohttp.ClientSession() as session:

        exchange_spec_dict = json.load(open('../admin/exchanges'))
        mapped_currency = exchange_spec_dict['currency_mapping']['wazirx']
        rest_url = exchange_spec_dict['source']['wazirx']['rest_url']

        url_dict = {'btcinr': f"{rest_url}depth?symbol={mapped_currency['btcinr']}&limit=10",
                    'ethinr': f"{rest_url}depth?symbol={mapped_currency['ethinr']}&limit=10",
                    'dogeinr': f"{rest_url}depth?symbol={mapped_currency['dogeinr']}r&limit=10",
                    'maticinr': f"{rest_url}depth?symbol={mapped_currency['maticinr']}&limit=10",
                    'adainr': f"{rest_url}depth?symbol={mapped_currency['adainr']}&limit=10",
                    'ftminr': f"{rest_url}depth?symbol={mapped_currency['ftminr']}&limit=10",
                    'xrpinr': f"{rest_url}depth?symbol={mapped_currency['xrpinr']}r&limit=10",
                    'sandinr': f"{rest_url}depth?symbol={mapped_currency['sandinr']}&limit=10",
                    'usdtinr': f"{rest_url}depth?symbol={mapped_currency['usdtinr']}&limit=10",
                    'solinr': f"{rest_url}depth?symbol={mapped_currency['solinr']}&limit=10",
                    'dotinr': f"{rest_url}depth?symbol={mapped_currency['dotinr']}r&limit=10",
                    'lunainr': f"{rest_url}depth?symbol={mapped_currency['lunainr']}&limit=10",
                    'trxinr': f"{rest_url}depth?symbol={mapped_currency['trxinr']}&limit=10",
                    'vetinr': f"{rest_url}depth?symbol={mapped_currency['vetinr']}&limit=10",
                    'lunausdt': f"{rest_url}depth?symbol={mapped_currency['lunausdt']}&limit=10",
                    'ethusdt': f"{rest_url}depth?symbol={mapped_currency['ethusdt']}&limit=10",
                    }

        while True:
            st = time.time()
            tasks = []
            for k in url_dict.keys():
                tasks.append(asyncio.ensure_future(single_url_getter(session, url_dict[k])))
                await asyncio.sleep(0.5)

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
