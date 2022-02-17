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
import datetime


async def single_url_getter(session, url):
    async with session.get(url) as response:
        message = await response.json()
        return message


def logging_handler():
    return logger_conf("../db_ex_connections/bitso.log")


async def main():
    cursor = connection()
    logger = logging_handler()
    async with aiohttp.ClientSession() as session:

        exchange_spec_dict = json.load(open('../admin/exchanges'))
        mapped_currency = exchange_spec_dict['currency_mapping']['bitso']
        rest_url = exchange_spec_dict['source']['bitso']['rest_url']

        url_dict = {'xrpmxn': f"{rest_url}/order_book/?book={mapped_currency['xrpmxn']}",
                    'btcmxn': f"{rest_url}/order_book/?book={mapped_currency['btcmxn']}",
                    'ethmxn': f"{rest_url}/order_book/?book={mapped_currency['ethmxn']}",
                    'manamxn': f"{rest_url}/order_book/?book={mapped_currency['manamxn']}",
                    'ltcusd': f"{rest_url}/order_book/?book={mapped_currency['ltcusd']}",
                    'btcbrl': f"{rest_url}/order_book/?book={mapped_currency['btcbrl']}",
                    'linkusd': f"{rest_url}/order_book/?book={mapped_currency['linkusd']}",
                    'batmxn': f"{rest_url}/order_book/?book={mapped_currency['batmxn']}",
                    'btcusd': f"{rest_url}/order_book/?book={mapped_currency['btcusd']}",
                    'xrpusd': f"{rest_url}/order_book/?book={mapped_currency['xrpusd']}",
                    'aaveusd': f"{rest_url}/order_book/?book={mapped_currency['aaveusd']}",
                    'ethbrl': f"{rest_url}/order_book/?book={mapped_currency['ethbrl']}",
                    'shibusd': f"{rest_url}/order_book/?book={mapped_currency['shibusd']}",
                    'sandusd': f"{rest_url}/order_book/?book={mapped_currency['sandusd']}",
                    'ftmusd': f"{rest_url}/order_book/?book={mapped_currency['ftmusd']}"}

        while True:
            try:
                st = time.time()
                tasks = []
                for k in url_dict.keys():
                    tasks.append(asyncio.create_task(single_url_getter(session, url_dict[k])))

                responses = await asyncio.gather(*tasks)
                for i in range(len(responses)):
                    if responses[i]['success'] is True:
                        before_db_save = time.time()

                        cursor.execute(f"""INSERT INTO bitso.{list(url_dict.keys())[i]}_ob ( ask_0, ask_vol_0, ask_1, 
                        ask_vol_1, ask_2, ask_vol_2, ask_3, ask_vol_3, ask_4, ask_vol_4, ask_5, ask_vol_5, ask_6, 
                        ask_vol_6, ask_7, ask_vol_7, ask_8, ask_vol_8, ask_9, ask_vol_9, bid_0, bid_vol_0, bid_1, 
                        bid_vol_1, bid_2, bid_vol_2, bid_3, bid_vol_3, bid_4, bid_vol_4, bid_5, bid_vol_5, bid_6, 
                        bid_vol_6, bid_7, bid_vol_7, bid_8, bid_vol_8, bid_9, bid_vol_9, "timestamp") VALUES ( 
                                                            {float(responses[i]['payload']['asks'][0]['price'])}, 
                                                            {float(responses[i]['payload']['asks'][0]['amount'])},
                                                            {float(responses[i]['payload']['asks'][1]['price'])},
                                                            {float(responses[i]['payload']['asks'][1]['amount'])},
                                                            {float(responses[i]['payload']['asks'][2]['price'])},
                                                            {float(responses[i]['payload']['asks'][2]['amount'])},
                                                            {float(responses[i]['payload']['asks'][3]['price'])},
                                                            {float(responses[i]['payload']['asks'][3]['amount'])},
                                                            {float(responses[i]['payload']['asks'][4]['price'])},
                                                            {float(responses[i]['payload']['asks'][4]['amount'])},
                                                            {float(responses[i]['payload']['asks'][5]['price'])},
                                                            {float(responses[i]['payload']['asks'][5]['amount'])},
                                                            {float(responses[i]['payload']['asks'][6]['price'])},
                                                            {float(responses[i]['payload']['asks'][6]['amount'])},
                                                            {float(responses[i]['payload']['asks'][7]['price'])},
                                                            {float(responses[i]['payload']['asks'][7]['amount'])},
                                                            {float(responses[i]['payload']['asks'][8]['price'])},
                                                            {float(responses[i]['payload']['asks'][8]['amount'])},
                                                            {float(responses[i]['payload']['asks'][9]['price'])},
                                                            {float(responses[i]['payload']['asks'][9]['amount'])},
                                                            {float(responses[i]['payload']['bids'][0]['price'])},
                                                            {float(responses[i]['payload']['bids'][0]['amount'])},
                                                            {float(responses[i]['payload']['bids'][1]['price'])},
                                                            {float(responses[i]['payload']['bids'][1]['amount'])},
                                                            {float(responses[i]['payload']['bids'][2]['price'])},
                                                            {float(responses[i]['payload']['bids'][2]['amount'])},
                                                            {float(responses[i]['payload']['bids'][3]['price'])},
                                                            {float(responses[i]['payload']['bids'][3]['amount'])},
                                                            {float(responses[i]['payload']['bids'][4]['price'])},
                                                            {float(responses[i]['payload']['bids'][4]['amount'])},
                                                            {float(responses[i]['payload']['bids'][5]['price'])},
                                                            {float(responses[i]['payload']['bids'][5]['amount'])},
                                                            {float(responses[i]['payload']['bids'][6]['price'])},
                                                            {float(responses[i]['payload']['bids'][6]['amount'])},
                                                            {float(responses[i]['payload']['bids'][7]['price'])},
                                                            {float(responses[i]['payload']['bids'][7]['amount'])},
                                                            {float(responses[i]['payload']['bids'][8]['price'])},
                                                            {float(responses[i]['payload']['bids'][8]['amount'])},
                                                            {float(responses[i]['payload']['bids'][9]['price'])},
                                                            {float(responses[i]['payload']['bids'][9]['amount'])},
                        {int(datetime.datetime.strptime(responses[i]['payload']['updated_at'].replace("T", " ").split("+")[0],
                                                        '%Y-%m-%d %H:%M:%S').timestamp())});""")

                        logger.debug(f"Time of saving ob for {k}: {time.time() - before_db_save}")


                    else:  # {'success': False, 'error': {'code': 200, 'message': 'Too many requests.'}}
                        logger.error(f" $$ Connection status: {str(responses[i]['error']['message'])} $$ ",
                                     exc_info=True)
                        time.sleep(5.0)
                if not [i['success'] for i in responses if i['success'] is False]:
                    logger.info(f"Ob received and successfully saved into database for {[*url_dict]}")

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
