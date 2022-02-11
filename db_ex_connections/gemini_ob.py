#!/usr/bin/env python3


import sys
sys.path.append("C:/Users/oskar/Desktop/hft_algo/hft_algo")
# sys.path.append("/home/obukowski/Desktop/repo/hft_algo")

import aiohttp
from aiohttp import ContentTypeError, ClientOSError, ClientConnectionError
import asyncio
import time
import json
from admin.admin_tools import connection, logger_conf


async def single_url_getter(session, url):
    async with session.get(url) as response:
        message = await response.json()
        return message


def logging_handler():
    return logger_conf("../db_ex_connections/gemini.log")


async def main():
    cursor = connection()
    logger = logging_handler()
    async with aiohttp.ClientSession() as session:

        exchange_spec_dict = json.load(open('../admin/exchanges'))
        mapped_currency = exchange_spec_dict['currency_mapping']['gemini']
        rest_url = exchange_spec_dict['source']['gemini']['rest_url']

        url_dict = {'btcusd': f"{rest_url}/book/{mapped_currency['btcusd']}",
                    'ethusd': f"{rest_url}/book/{mapped_currency['ethusd']}",
                    'dogeusd': f"{rest_url}/book/{mapped_currency['dogeusd']}",
                    'maticusd': f"{rest_url}/book/{mapped_currency['maticusd']}",
                    'sushiusd': f"{rest_url}/book/{mapped_currency['sushiusd']}",
                    'ftmusd': f"{rest_url}/book/{mapped_currency['ftmusd']}",
                    'linkusd': f"{rest_url}/book/{mapped_currency['linkusd']}",
                    'sandusd': f"{rest_url}/book/{mapped_currency['sandusd']}",
                    'filusd': f"{rest_url}/book/{mapped_currency['filusd']}",
                    'galausd': f"{rest_url}/book/{mapped_currency['galausd']}",
                    'manausd': f"{rest_url}/book/{mapped_currency['manausd']}",
                    'lrcusd': f"{rest_url}/book/{mapped_currency['lrcusd']}",
                    'lunausd': f"{rest_url}/book/{mapped_currency['lunausd']}",
                    'crvusd': f"{rest_url}/book/{mapped_currency['crvusd']}",
                    'aaveusd': f"{rest_url}/book/{mapped_currency['aaveusd']}",
                    'uniusd': f"{rest_url}/book/{mapped_currency['uniusd']}",
                    'axsusd': f"{rest_url}/book/{mapped_currency['axsusd']}"
                    }

        while True:
            try:

                st = time.time()
                tasks = []
                for k in url_dict.keys():
                    tasks.append(asyncio.create_task(single_url_getter(session, url_dict[k])))

                responses = await asyncio.gather(*tasks)
                for i in range(len(responses)):
                    before_db_save = time.time()
                    cursor.execute(f"""INSERT INTO gemini.{list(url_dict.keys())[i]}_ob (
                    ask_0, ask_vol_0, ask_1, ask_vol_1, ask_2, ask_vol_2, ask_3, ask_vol_3, ask_4, ask_vol_4,
                    ask_5, ask_vol_5, ask_6, ask_vol_6, ask_7, ask_vol_7, ask_8, ask_vol_8, ask_9, ask_vol_9,
                    bid_0, bid_vol_0, bid_1, bid_vol_1, bid_2, bid_vol_2, bid_3, bid_vol_3, bid_4, bid_vol_4,
                    bid_5, bid_vol_5, bid_6, bid_vol_6, bid_7, bid_vol_7, bid_8, bid_vol_8, bid_9, bid_vol_9, "timestamp")
                                                    VALUES (
                                                        {float(responses[i]['asks'][0]['price'])},
                                                        {float(responses[i]['asks'][0]['amount'])},
                                                        {float(responses[i]['asks'][1]['price'])},
                                                        {float(responses[i]['asks'][1]['amount'])},
                                                        {float(responses[i]['asks'][2]['price'])},
                                                        {float(responses[i]['asks'][2]['amount'])},
                                                        {float(responses[i]['asks'][3]['price'])},
                                                        {float(responses[i]['asks'][3]['amount'])},
                                                        {float(responses[i]['asks'][4]['price'])},
                                                        {float(responses[i]['asks'][4]['amount'])},
                                                        {float(responses[i]['asks'][5]['price'])},
                                                        {float(responses[i]['asks'][5]['amount'])},
                                                        {float(responses[i]['asks'][6]['price'])},
                                                        {float(responses[i]['asks'][6]['amount'])},
                                                        {float(responses[i]['asks'][7]['price'])},
                                                        {float(responses[i]['asks'][7]['amount'])},
                                                        {float(responses[i]['asks'][8]['price'])},
                                                        {float(responses[i]['asks'][8]['amount'])},
                                                        {float(responses[i]['asks'][9]['price'])},
                                                        {float(responses[i]['asks'][9]['amount'])},
                                                        {float(responses[i]['bids'][0]['price'])},
                                                        {float(responses[i]['bids'][0]['amount'])},
                                                        {float(responses[i]['bids'][1]['price'])},
                                                        {float(responses[i]['bids'][1]['amount'])},
                                                        {float(responses[i]['bids'][2]['price'])},
                                                        {float(responses[i]['bids'][2]['amount'])},
                                                        {float(responses[i]['bids'][3]['price'])},
                                                        {float(responses[i]['bids'][3]['amount'])},
                                                        {float(responses[i]['bids'][4]['price'])},
                                                        {float(responses[i]['bids'][4]['amount'])},
                                                        {float(responses[i]['bids'][5]['price'])},
                                                        {float(responses[i]['bids'][5]['amount'])},
                                                        {float(responses[i]['bids'][6]['price'])},
                                                        {float(responses[i]['bids'][6]['amount'])},
                                                        {float(responses[i]['bids'][7]['price'])},
                                                        {float(responses[i]['bids'][7]['amount'])},
                                                        {float(responses[i]['bids'][8]['price'])},
                                                        {float(responses[i]['bids'][8]['amount'])},
                                                        {float(responses[i]['bids'][9]['price'])},
                                                        {float(responses[i]['bids'][9]['amount'])},
                                                        {int(responses[i]['bids'][9]['timestamp'])});""")

                    logger.debug(f"Time of saving ob for {list(url_dict.keys())[i]}: {time.time() - before_db_save}")
                logger.info(f"Ob received and successfully saved into database for {[*url_dict]} ")

                await asyncio.sleep(5 - (time.time() - st))

            except (KeyError, RuntimeError, ContentTypeError) as rest_error:
                logger.error(f" $$ {str(rest_error)} $$ ", exc_info=True)

if __name__ == '__main__':
    while True:
        try:
            asyncio.run(main())
        except (RuntimeError, KeyboardInterrupt, ClientConnectionError, ClientOSError) as kill:
            logging_handler().error(f" $$ System's try to kill process, error: {str(kill)} $$ ", exc_info=True)
            continue
