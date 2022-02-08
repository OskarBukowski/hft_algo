#!/usr/bin/env python3

###
# TO DO:
# 1. handle with rate limit errors:
# {'code': 2136, 'message': 'Too many api request'}


import sys
sys.path.append("C:/Users/oskar/Desktop/hft_algo/hft_algo")

import asyncio
from admin.admin_tools import connection, logger_conf
import time
import json
import aiohttp
from aiohttp import ContentTypeError


async def single_url_getter(session, url):
    async with session.get(url) as response:
        message = await response.json()
        return message


async def main():
    cursor = connection()
    logger = logger_conf("../db_ex_connections/huobi.log")
    async with aiohttp.ClientSession() as session:

        exchange_spec_dict = json.load(open('../admin/exchanges'))
        mapped_currency = exchange_spec_dict['currency_mapping']['huobi']
        rest_url = exchange_spec_dict['source']['huobi']['rest_url']

        url_dict = {'btcusdt': f"{rest_url}market/depth?symbol={mapped_currency['btcusdt']}&type=step0&depth=10",
                    'ethusdt': f"{rest_url}market/depth?symbol={mapped_currency['ethusdt']}&type=step0&depth=10",
                    'shibusdt': f"{rest_url}market/depth?symbol={mapped_currency['shibusdt']}&type=step0&depth=10",
                    'avaxusdt': f"{rest_url}market/depth?symbol={mapped_currency['avaxusdt']}&type=step0&depth=10",
                    'filusdt': f"{rest_url}market/depth?symbol={mapped_currency['filusdt']}&type=step0&depth=10",
                    'adausdt': f"{rest_url}market/depth?symbol={mapped_currency['adausdt']}&type=step0&depth=10",
                    'solusdt': f"{rest_url}market/depth?symbol={mapped_currency['solusdt']}&type=step0&depth=10",
                    'xrpusdt': f"{rest_url}market/depth?symbol={mapped_currency['xrpusdt']}&type=step0&depth=10",
                    'trxusdt': f"{rest_url}market/depth?symbol={mapped_currency['trxusdt']}&type=step0&depth=10",
                    'galausdt': f"{rest_url}market/depth?symbol={mapped_currency['galausdt']}&type=step0&depth=10",
                    'manausdt': f"{rest_url}market/depth?symbol={mapped_currency['manausdt']}&type=step0&depth=10",
                    'dotusdt': f"{rest_url}market/depth?symbol={mapped_currency['dotusdt']}&type=step0&depth=10",
                    'lunausdt': f"{rest_url}market/depth?symbol={mapped_currency['lunausdt']}&type=step0&depth=10",
                    'sandusdt': f"{rest_url}market/depth?symbol={mapped_currency['sandusdt']}&type=step0&depth=10",
                    'dogeusdt': f"{rest_url}market/depth?symbol={mapped_currency['dogeusdt']}&type=step0&depth=10",
                    'axsusdt': f"{rest_url}market/depth?symbol={mapped_currency['axsusdt']}&type=step0&depth=10",
                    'maticusdt': f"{rest_url}market/depth?symbol={mapped_currency['maticusdt']}&type=step0&depth=10"
                    }

        while True:
            try:
                st = time.time()
                tasks = []
                for k in url_dict.keys():
                    tasks.append(asyncio.create_task(single_url_getter(session, url_dict[k])))
                    await asyncio.sleep(1)

                responses = await asyncio.gather(*tasks)

                if responses[0]['status'] == 'ok':
                    print(responses)


            #         except KeyError:
            #             before_db_save = time.time()
            #             cursor.execute(f"""INSERT INTO wazirx.{list(url_dict.keys())[i]}_ob (
            #                 ask_0, ask_vol_0, ask_1, ask_vol_1, ask_2, ask_vol_2, ask_3, ask_vol_3, ask_4, ask_vol_4,
            #                 ask_5, ask_vol_5, ask_6, ask_vol_6, ask_7, ask_vol_7, ask_8, ask_vol_8, ask_9, ask_vol_9,
            #                 bid_0, bid_vol_0, bid_1, bid_vol_1, bid_2, bid_vol_2, bid_3, bid_vol_3, bid_4, bid_vol_4,
            #                 bid_5, bid_vol_5, bid_6, bid_vol_6, bid_7, bid_vol_7, bid_8, bid_vol_8, bid_9, bid_vol_9, "timestamp")
            #                                         VALUES (
            #                                                 {float(responses[i]['asks'][0][0])},
            #                                                 {float(responses[i]['asks'][0][1])},
            #                                                 {float(responses[i]['asks'][1][0])},
            #                                                 {float(responses[i]['asks'][1][1])},
            #                                                 {float(responses[i]['asks'][2][0])},
            #                                                 {float(responses[i]['asks'][2][1])},
            #                                                 {float(responses[i]['asks'][3][0])},
            #                                                 {float(responses[i]['asks'][3][1])},
            #                                                 {float(responses[i]['asks'][4][0])},
            #                                                 {float(responses[i]['asks'][4][1])},
            #                                                 {float(responses[i]['asks'][5][0])},
            #                                                 {float(responses[i]['asks'][5][1])},
            #                                                 {float(responses[i]['asks'][6][0])},
            #                                                 {float(responses[i]['asks'][6][1])},
            #                                                 {float(responses[i]['asks'][7][0])},
            #                                                 {float(responses[i]['asks'][7][1])},
            #                                                 {float(responses[i]['asks'][8][0])},
            #                                                 {float(responses[i]['asks'][8][1])},
            #                                                 {float(responses[i]['asks'][9][0])},
            #                                                 {float(responses[i]['asks'][9][1])},
            #                                                 {float(responses[i]['bids'][0][0])},
            #                                                 {float(responses[i]['bids'][0][1])},
            #                                                 {float(responses[i]['bids'][1][0])},
            #                                                 {float(responses[i]['bids'][1][1])},
            #                                                 {float(responses[i]['bids'][2][0])},
            #                                                 {float(responses[i]['bids'][2][1])},
            #                                                 {float(responses[i]['bids'][3][0])},
            #                                                 {float(responses[i]['bids'][3][1])},
            #                                                 {float(responses[i]['bids'][4][0])},
            #                                                 {float(responses[i]['bids'][4][1])},
            #                                                 {float(responses[i]['bids'][5][0])},
            #                                                 {float(responses[i]['bids'][5][1])},
            #                                                 {float(responses[i]['bids'][6][0])},
            #                                                 {float(responses[i]['bids'][6][1])},
            #                                                 {float(responses[i]['bids'][7][0])},
            #                                                 {float(responses[i]['bids'][7][1])},
            #                                                 {float(responses[i]['bids'][8][0])},
            #                                                 {float(responses[i]['bids'][8][1])},
            #                                                 {float(responses[i]['bids'][9][0])},
            #                                                 {float(responses[i]['bids'][9][1])},
            #                                                 {int(responses[i]['timestamp'])});""")
            #
            #         logger.debug(f"Time of saving ob for {k}: {time.time() - before_db_save}")
            #       logger.info(f"Ob received and successfully saved into database for {[*url_dict]} ")

                else:  # {"status": "Fail"} or other unexpected REST API responses
                    logger.error(f" $$ Connection status: {str(responses[0]['status'])} $$ ", exc_info=True)
            #
            #     await asyncio.sleep(5 - (time.time() - st))
            #
            except (KeyError, RuntimeError, ContentTypeError) as rest_error:
                logger.error(f" $$ {str(rest_error)} $$ ", exc_info=True)


asyncio.run(main())
