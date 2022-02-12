#!/usr/bin/env python3



import sys
sys.path.append("C:/Users/oskar/Desktop/hft_algo/")
sys.path.append("/home/obukowski/Desktop/repo/hft_algo")

import aiohttp
from aiohttp import ContentTypeError, ClientConnectionError, ClientOSError
import asyncio
from asyncio.exceptions import TimeoutError
import time
import json
from admin.admin_tools import connection, logger_conf


async def single_url_getter(session, url):
    async with session.get(url) as response:
        message = await response.json()
        return message


def logging_handler():
    return logger_conf("../db_ex_connections/zonda.log")


async def main():
    cursor = connection()
    logger = logging_handler()
    async with aiohttp.ClientSession() as session:

        exchange_spec_dict = json.load(open('../admin/exchanges'))
        mapped_currency = exchange_spec_dict['currency_mapping']['zonda']
        rest_url = exchange_spec_dict['source']['zonda']['rest_url']

        url_dict = {'btcpln': f"{rest_url}trading/orderbook-limited/{mapped_currency['btcpln']}/10",
                    'ethpln': f"{rest_url}trading/orderbook-limited/{mapped_currency['ethpln']}/10",
                    'lunapln': f"{rest_url}trading/orderbook-limited/{mapped_currency['lunapln']}/10",
                    'ftmpln': f"{rest_url}trading/orderbook-limited/{mapped_currency['ftmpln']}/10",
                    'btceur': f"{rest_url}trading/orderbook-limited/{mapped_currency['btceur']}/10",
                    'xrppln': f"{rest_url}trading/orderbook-limited/{mapped_currency['xrppln']}/10",
                    'etheur': f"{rest_url}trading/orderbook-limited/{mapped_currency['etheur']}/10",
                    'adapln': f"{rest_url}trading/orderbook-limited/{mapped_currency['adapln']}/10",
                    'maticpln': f"{rest_url}trading/orderbook-limited/{mapped_currency['maticpln']}/10",
                    'usdtpln': f"{rest_url}trading/orderbook-limited/{mapped_currency['usdtpln']}/10",
                    'dotpln': f"{rest_url}trading/orderbook-limited/{mapped_currency['dotpln']}/10",
                    'avaxpln': f"{rest_url}trading/orderbook-limited/{mapped_currency['avaxpln']}/10",
                    'dogepln': f"{rest_url}trading/orderbook-limited/{mapped_currency['dogepln']}/10",
                    'trxpln': f"{rest_url}trading/orderbook-limited/{mapped_currency['trxpln']}/10",
                    'manapln': f"{rest_url}trading/orderbook-limited/{mapped_currency['manapln']}/10",
                    'linkpln': f"{rest_url}trading/orderbook-limited/{mapped_currency['linkpln']}/10"
                    }

        while True:
            try:
                st = time.time()
                tasks = []
                for k in url_dict.keys():
                    tasks.append(asyncio.create_task(single_url_getter(session, url_dict[k])))

                responses = await asyncio.gather(*tasks)

                if responses[0]['status'] == "Ok":
                    before_db_save = time.time()

                    for i in range(0, len(responses)):
                        cursor.execute(f"""INSERT INTO zonda.{list(url_dict.keys())[i]}_ob (
                        ask_0, ask_vol_0, ask_1, ask_vol_1, ask_2, ask_vol_2, ask_3, ask_vol_3, ask_4, ask_vol_4, 
                        ask_5, ask_vol_5, ask_6, ask_vol_6, ask_7, ask_vol_7, ask_8, ask_vol_8, ask_9, ask_vol_9, 
                        bid_0, bid_vol_0, bid_1, bid_vol_1, bid_2, bid_vol_2, bid_3, bid_vol_3, bid_4, bid_vol_4, 
                        bid_5, bid_vol_5, bid_6, bid_vol_6, bid_7, bid_vol_7, bid_8, bid_vol_8, bid_9, bid_vol_9, "timestamp")
                                                        VALUES (
                                                            {float(responses[i]['sell'][0]['ra'])},
                                                            {float(responses[i]['sell'][0]['ca'])},
                                                            {float(responses[i]['sell'][1]['ra'])},
                                                            {float(responses[i]['sell'][1]['ca'])},
                                                            {float(responses[i]['sell'][2]['ra'])},
                                                            {float(responses[i]['sell'][2]['ca'])},
                                                            {float(responses[i]['sell'][3]['ra'])},
                                                            {float(responses[i]['sell'][3]['ca'])},
                                                            {float(responses[i]['sell'][4]['ra'])},
                                                            {float(responses[i]['sell'][4]['ca'])},
                                                            {float(responses[i]['sell'][5]['ra'])},
                                                            {float(responses[i]['sell'][5]['ca'])},
                                                            {float(responses[i]['sell'][6]['ra'])},
                                                            {float(responses[i]['sell'][6]['ca'])},
                                                            {float(responses[i]['sell'][7]['ra'])},
                                                            {float(responses[i]['sell'][7]['ca'])},
                                                            {float(responses[i]['sell'][8]['ra'])},
                                                            {float(responses[i]['sell'][8]['ca'])},
                                                            {float(responses[i]['sell'][9]['ra'])},
                                                            {float(responses[i]['sell'][9]['ca'])},
                                                            {float(responses[i]['buy'][0]['ra'])},
                                                            {float(responses[i]['buy'][0]['ca'])},
                                                            {float(responses[i]['buy'][1]['ra'])},
                                                            {float(responses[i]['buy'][1]['ca'])},
                                                            {float(responses[i]['buy'][2]['ra'])},
                                                            {float(responses[i]['buy'][2]['ca'])},
                                                            {float(responses[i]['buy'][3]['ra'])},
                                                            {float(responses[i]['buy'][3]['ca'])},
                                                            {float(responses[i]['buy'][4]['ra'])},
                                                            {float(responses[i]['buy'][4]['ca'])},
                                                            {float(responses[i]['buy'][5]['ra'])},
                                                            {float(responses[i]['buy'][5]['ca'])},
                                                            {float(responses[i]['buy'][6]['ra'])},
                                                            {float(responses[i]['buy'][6]['ca'])},
                                                            {float(responses[i]['buy'][7]['ra'])},
                                                            {float(responses[i]['buy'][7]['ca'])},
                                                            {float(responses[i]['buy'][8]['ra'])},
                                                            {float(responses[i]['buy'][8]['ca'])},
                                                            {float(responses[i]['buy'][9]['ra'])},
                                                            {float(responses[i]['buy'][9]['ca'])},
                                                            {int(responses[i]['timestamp'])});""")

                    logger.debug(f"Time of saving ob for {k}: {time.time() - before_db_save}")
                    logger.info(f"Ob received and successfully saved into database for {[*url_dict]} ")

                else:  # {"status": "Fail"} or other unexpected REST API responses
                    logger.error(f" $$ Connection status: {str(responses[0]['status'])} $$ ", exc_info=True)
                    time.sleep(5.0)

                await asyncio.sleep(5 - (time.time() - st))

            except (KeyError, ContentTypeError) as rest_error:
                logger.error(f" $$ {str(rest_error)} $$ ", exc_info=True)
                continue

if __name__ == '__main__':
    while True:
        try:
            asyncio.run(main())
        except (RuntimeError, KeyboardInterrupt, ClientConnectionError, ClientOSError, TimeoutError) as kill:
            logging_handler().error(f" $$ Connection kill, error: {str(kill)} $$ ", exc_info=True)
            continue



