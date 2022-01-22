#!/usr/bin/env python3


#####
# This file show request using REST API
# It returns up to 10 buy and sell offer on current moment
# It saves 5 lines of orderbook
#####

### MERGE TO MASTER



import aiohttp
import asyncio
from admin_tools.admin_tools import connection, logger_conf
import time
import json


async def single_url_getter(session, url):
    async with session.get(url) as response:
        message = await response.json()
        return message


async def main():
    cursor = connection()
    logger = logger_conf("../db_ex_connections/zonda.log")
    async with aiohttp.ClientSession() as session:

        exchange_spec_dict = json.load(open('../admin_tools/exchanges'))
        mapped_currency = exchange_spec_dict['currency_mapping'][0]['symbols']
        rest_url = exchange_spec_dict['source'][0]['rest_url']

        url_dict = {'btcpln': f"{rest_url}trading/orderbook-limited/{mapped_currency['btcpln']}/10",
                    'ethpln': f"{rest_url}trading/orderbook-limited/{mapped_currency['ethpln']}/10",
                    'lunapln': f"{rest_url}trading/orderbook-limited/{mapped_currency['lunapln']}/10",
                    'ftmpln': f"{rest_url}trading/orderbook-limited/{mapped_currency['ftmpln']}/10"}

        while True:
            try:
                st = time.time()
                tasks = []
                for k in url_dict.keys():
                    st = time.time()
                    tasks.append(asyncio.create_task(single_url_getter(session, url_dict[k])))

                responses = await asyncio.gather(*tasks)

                if responses[0]['status'] == "Ok":
                    print(responses)
                    before_db_save = time.time()

                    for i in range(0, len(responses)):
                        cursor.execute(f"""INSERT INTO zonda.{list(url_dict.keys())[i]}_ob (ask_0, ask_1, 
                                                    ask_2, ask_3, ask_4, ask_vol_0, ask_vol_1, ask_vol_2,
                                                    ask_vol_3, ask_vol_4, bid_0, bid_1, bid_2, bid_3, bid_4, bid_vol_0, 
                                                    bid_vol_1, bid_vol_2, bid_vol_3,bid_vol_4, "timestamp")
                                                        VALUES (
                                                            {float(responses[i]['sell'][0]['ra'])},
                                                            {float(responses[i]['sell'][1]['ra'])},
                                                            {float(responses[i]['sell'][2]['ra'])},
                                                            {float(responses[i]['sell'][3]['ra'])},
                                                            {float(responses[i]['sell'][4]['ra'])},
                                                            {float(responses[i]['sell'][0]['ca'])},
                                                            {float(responses[i]['sell'][1]['ca'])},
                                                            {float(responses[i]['sell'][2]['ca'])},
                                                            {float(responses[i]['sell'][3]['ca'])},
                                                            {float(responses[i]['sell'][4]['ca'])},
                                                            {float(responses[i]['buy'][0]['ra'])},
                                                            {float(responses[i]['buy'][1]['ra'])},
                                                            {float(responses[i]['buy'][2]['ra'])},
                                                            {float(responses[i]['buy'][3]['ra'])},
                                                            {float(responses[i]['buy'][4]['ra'])},
                                                            {float(responses[i]['buy'][0]['ca'])},
                                                            {float(responses[i]['buy'][1]['ca'])},
                                                            {float(responses[i]['buy'][2]['ca'])},
                                                            {float(responses[i]['buy'][3]['ca'])},
                                                            {float(responses[i]['buy'][4]['ca'])},
                                                            {int(responses[i]['timestamp'])});""")

                    logger.debug(f"Time of saving ob for {k}: {time.time() - before_db_save}")
                    logger.info(f"Ob received and successfully saved into database for {[*url_dict]} ")

                else:  # {"status": "Fail"} or other unexpected REST API responses
                    logger.error(f" $$ Connection status: {responses[0]['status']} $$ ", exc_info=True)

                await asyncio.sleep(5 - (time.time() - st))

            except (KeyError, RuntimeError) as rest_error:
                logger.error(f" $$ {rest_error} $$ ", exc_info=True)


asyncio.run(main())
