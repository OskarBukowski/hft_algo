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
                    'dogeinr': f"{rest_url}depth?symbol={mapped_currency['dogeinr']}&limit=10",
                    'maticinr': f"{rest_url}depth?symbol={mapped_currency['maticinr']}&limit=10",
                    'adainr': f"{rest_url}depth?symbol={mapped_currency['adainr']}&limit=10",
                    'ftminr': f"{rest_url}depth?symbol={mapped_currency['ftminr']}&limit=10",
                    'xrpinr': f"{rest_url}depth?symbol={mapped_currency['xrpinr']}&limit=10",
                    'sandinr': f"{rest_url}depth?symbol={mapped_currency['sandinr']}&limit=10",
                    'usdtinr': f"{rest_url}depth?symbol={mapped_currency['usdtinr']}&limit=10",
                    'solinr': f"{rest_url}depth?symbol={mapped_currency['solinr']}&limit=10",
                    'manainr': f"{rest_url}depth?symbol={mapped_currency['manainr']}&limit=10",
                    'dotinr': f"{rest_url}depth?symbol={mapped_currency['dotinr']}&limit=10",
                    'lunainr': f"{rest_url}depth?symbol={mapped_currency['lunainr']}&limit=10",
                    'trxinr': f"{rest_url}depth?symbol={mapped_currency['trxinr']}&limit=10",
                    'vetinr': f"{rest_url}depth?symbol={mapped_currency['vetinr']}&limit=10",
                    'lunausdt': f"{rest_url}depth?symbol={mapped_currency['lunausdt']}&limit=10",
                    'ethusdt': f"{rest_url}depth?symbol={mapped_currency['ethusdt']}&limit=10",
                    }

        while True:
            try:
                st = time.time()
                tasks = []
                for k in url_dict.keys():
                    tasks.append(asyncio.ensure_future(single_url_getter(session, url_dict[k])))
                    await asyncio.sleep(1)

                responses = await asyncio.gather(*tasks)
                for i in range(len(responses)):
                    try:
                        if isinstance(responses[i]['code'], int):
                            logger.error(f" $$ {str(responses[i]['message'])} $$ ", exc_info=True)
                            logger.warning(f"Check the configuration for {list(url_dict.keys())[i]}")

                    except KeyError:
                        before_db_save = time.time()
                        cursor.execute(f"""INSERT INTO wazirx.{list(url_dict.keys())[i]}_ob (
                            ask_0, ask_vol_0, ask_1, ask_vol_1, ask_2, ask_vol_2, ask_3, ask_vol_3, ask_4, ask_vol_4,
                            ask_5, ask_vol_5, ask_6, ask_vol_6, ask_7, ask_vol_7, ask_8, ask_vol_8, ask_9, ask_vol_9,
                            bid_0, bid_vol_0, bid_1, bid_vol_1, bid_2, bid_vol_2, bid_3, bid_vol_3, bid_4, bid_vol_4,
                            bid_5, bid_vol_5, bid_6, bid_vol_6, bid_7, bid_vol_7, bid_8, bid_vol_8, bid_9, bid_vol_9, "timestamp")
                                                    VALUES (
                                                            {float(responses[i]['asks'][0][0])},
                                                            {float(responses[i]['asks'][0][1])},
                                                            {float(responses[i]['asks'][1][0])},
                                                            {float(responses[i]['asks'][1][1])},
                                                            {float(responses[i]['asks'][2][0])},
                                                            {float(responses[i]['asks'][2][1])},
                                                            {float(responses[i]['asks'][3][0])},
                                                            {float(responses[i]['asks'][3][1])},
                                                            {float(responses[i]['asks'][4][0])},
                                                            {float(responses[i]['asks'][4][1])},
                                                            {float(responses[i]['asks'][5][0])},
                                                            {float(responses[i]['asks'][5][1])},
                                                            {float(responses[i]['asks'][6][0])},
                                                            {float(responses[i]['asks'][6][1])},
                                                            {float(responses[i]['asks'][7][0])},
                                                            {float(responses[i]['asks'][7][1])},
                                                            {float(responses[i]['asks'][8][0])},
                                                            {float(responses[i]['asks'][8][1])},
                                                            {float(responses[i]['asks'][9][0])},
                                                            {float(responses[i]['asks'][9][1])},
                                                            {float(responses[i]['bids'][0][0])},
                                                            {float(responses[i]['bids'][0][1])},
                                                            {float(responses[i]['bids'][1][0])},
                                                            {float(responses[i]['bids'][1][1])},
                                                            {float(responses[i]['bids'][2][0])},
                                                            {float(responses[i]['bids'][2][1])},
                                                            {float(responses[i]['bids'][3][0])},
                                                            {float(responses[i]['bids'][3][1])},
                                                            {float(responses[i]['bids'][4][0])},
                                                            {float(responses[i]['bids'][4][1])},
                                                            {float(responses[i]['bids'][5][0])},
                                                            {float(responses[i]['bids'][5][1])},
                                                            {float(responses[i]['bids'][6][0])},
                                                            {float(responses[i]['bids'][6][1])},
                                                            {float(responses[i]['bids'][7][0])},
                                                            {float(responses[i]['bids'][7][1])},
                                                            {float(responses[i]['bids'][8][0])},
                                                            {float(responses[i]['bids'][8][1])},
                                                            {float(responses[i]['bids'][9][0])},
                                                            {float(responses[i]['bids'][9][1])},
                                                            {int(responses[i]['timestamp'])});""")

                    logger.debug(f"Time of saving ob for {k}: {time.time() - before_db_save}")
                logger.info(f"Ob received and successfully saved into database for {[*url_dict]} ")

                await asyncio.sleep(5 - (time.time() - st))

            except (KeyError, RuntimeError) as rest_error:
                logger.error(f" $$ {str(rest_error)} $$ ", exc_info=True)


asyncio.run(main())
