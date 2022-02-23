#!/usr/bin/env python3


import sys

import numpy as np

sys.path.append("//")
sys.path.append("/home/obukowski/Desktop/repo/hft_algo")

import aiohttp
from aiohttp import ContentTypeError, ClientConnectionError, ClientOSError
import asyncio
from asyncio.exceptions import TimeoutError
import time
import json
from admin.admin_tools import connection, logger_conf
import time


async def single_url_getter(session, url):
    async with session.get(url) as response:
        message = await response.json()
        return message


def logging_handler():
    return logger_conf("../bitfinex/bitfinex.log")


async def main():
    cursor = connection()
    logger = logging_handler()
    async with aiohttp.ClientSession() as session:

        exchange_spec_dict = json.load(open('../../admin/exchanges'))
        mapped_currency = exchange_spec_dict['currency_mapping']['bitfinex']
        rest_url = exchange_spec_dict['source']['bitfinex']['rest_url']

        url_dict = {'btcusd': f"{rest_url}/v2/book/{mapped_currency['btcusd']}/P0",
                    'ethusd': f"{rest_url}/v2/book/{mapped_currency['ethusd']}/P0",
                    'shibusd': f"{rest_url}/v2/book/{mapped_currency['shibusd']}/P0",
                    'avaxusd': f"{rest_url}/v2/book/{mapped_currency['avaxusd']}/P0",
                    'filusd': f"{rest_url}/v2/book/{mapped_currency['filusd']}/P0",
                    'adausd': f"{rest_url}/v2/book/{mapped_currency['adausd']}/P0",
                    'solusd': f"{rest_url}/v2/book/{mapped_currency['solusd']}/P0",
                    'xrpusd': f"{rest_url}/v2/book/{mapped_currency['xrpusd']}/P0",
                    'linkusd': f"{rest_url}/v2/book/{mapped_currency['linkusd']}/P0",
                    'ltcusd': f"{rest_url}/v2/book/{mapped_currency['ltcusd']}/P0",
                    'dotusd': f"{rest_url}/v2/book/{mapped_currency['dotusd']}/P0",
                    'lunausdt': f"{rest_url}/v2/book/{mapped_currency['lunausdt']}/P0",
                    'uniusd': f"{rest_url}/v2/book/{mapped_currency['uniusd']}/P0",
                    'dogeusd': f"{rest_url}/v2/book/{mapped_currency['dogeusd']}/P0",
                    'dogeusdt': f"{rest_url}/v2/book/{mapped_currency['dogeusdt']}/P0",
                    'maticusd': f"{rest_url}/v2/book/{mapped_currency['maticusd']}/P0"
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
                    print(responses[i][0])
                    asks = [r for r in responses[i] if float(r[2]) < 0]
                    bids = [r for r in responses[i] if float(r[2]) > 0]

                    cursor.execute(f"""INSERT INTO bitfinex.{list(url_dict.keys())[i]}_ob ( ask_0, ask_vol_0, ask_1,
                    ask_vol_1, ask_2, ask_vol_2, ask_3, ask_vol_3, ask_4, ask_vol_4, ask_5, ask_vol_5, ask_6,
                    ask_vol_6, ask_7, ask_vol_7, ask_8, ask_vol_8, ask_9, ask_vol_9, bid_0, bid_vol_0, bid_1,
                    bid_vol_1, bid_2, bid_vol_2, bid_3, bid_vol_3, bid_4, bid_vol_4, bid_5, bid_vol_5, bid_6,
                    bid_vol_6, bid_7, bid_vol_7, bid_8, bid_vol_8, bid_9, bid_vol_9, "timestamp") VALUES (
                                                        {float(asks[0][0])},
                                                        {np.absolute(float(asks[0][2]))},
                                                        {float(asks[1][0])},
                                                        {np.absolute(float(asks[1][2]))},
                                                        {float(asks[2][0])},
                                                        {np.absolute(float(asks[2][2]))},
                                                        {float(asks[3][0])},
                                                        {np.absolute(float(asks[3][2]))},
                                                        {float(asks[4][0])},
                                                        {np.absolute(float(asks[4][2]))},
                                                        {float(asks[5][0])},
                                                        {np.absolute(float(asks[5][2]))},
                                                        {float(asks[6][0])},
                                                        {np.absolute(float(asks[6][2]))},
                                                        {float(asks[7][0])},
                                                        {np.absolute(float(asks[7][2]))},
                                                        {float(asks[8][0])},
                                                        {np.absolute(float(asks[8][2]))},
                                                        {float(asks[9][0])},
                                                        {np.absolute(float(asks[9][2]))},
                                                        {float(bids[0][0])},
                                                        {float(bids[0][2])},
                                                        {float(bids[1][0])},
                                                        {float(bids[1][2])},
                                                        {float(bids[2][0])},
                                                        {float(bids[2][2])},
                                                        {float(bids[3][0])},
                                                        {float(bids[3][2])},
                                                        {float(bids[4][0])},
                                                        {float(bids[4][2])},
                                                        {float(bids[5][0])},
                                                        {float(bids[5][2])},
                                                        {float(bids[6][0])},
                                                        {float(bids[6][2])},
                                                        {float(bids[7][0])},
                                                        {float(bids[7][2])},
                                                        {float(bids[8][0])},
                                                        {float(bids[8][2])},
                                                        {float(bids[9][0])},
                                                        {float(bids[9][2])},
                                                        {int(round(time.time() * 1000))});""")

                logger.debug(f"Time of saving ob for {[*url_dict]}: {time.time() - before_db_save}")
                logger.info(f"Ob received and successfully saved into database for {[*url_dict]} ")


                await asyncio.sleep(5 - (time.time() - st))

            except (KeyError, TypeError, ContentTypeError, ValueError) as rest_error:
                logger.error(f" $$ {str(rest_error)} $$ ", exc_info=True)
                continue


if __name__ == '__main__':
    while True:
        try:
            asyncio.run(main())
        except (RuntimeError, KeyboardInterrupt, ClientConnectionError, ClientOSError, TimeoutError) as kill:
            logging_handler().error(f" $$ Connection kill, error: {str(kill)} $$ ", exc_info=True)
            continue
