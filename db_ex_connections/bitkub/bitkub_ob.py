#!/usr/bin/env python3

import sys
sys.path.append("C:/Users/oskar/Desktop/hft_algo/hft_algo")
sys.path.append("/home/obukowski/Desktop/repo/hft_algo")

import asyncio
from asyncio.exceptions import TimeoutError
import time
import json
from admin.admin_tools import connection, logger_conf, dict_values_getter
import aiohttp
from aiohttp import ContentTypeError, ClientOSError, ClientConnectionError


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


def logging_handler():
    return logger_conf("../bitkub/bitkub.log")


async def main():
    cursor = connection()
    logger = logging_handler()

    async with aiohttp.ClientSession() as session:

        exchange_spec_dict = json.load(open('../../admin/exchanges'))
        mapped_currency = exchange_spec_dict['currency_mapping']['bitkub']
        rest_url = exchange_spec_dict['source']['bitkub']['rest_url']

        url_dict = {
            'btcthb': [
                f"{rest_url}market/asks?sym={mapped_currency['btcthb']}&lmt=10",
                f"{rest_url}market/bids?sym={mapped_currency['btcthb']}&lmt=10"],
            'eththb': [
                f"{rest_url}market/asks?sym={mapped_currency['eththb']}&lmt=10",
                f"{rest_url}market/bids?sym={mapped_currency['eththb']}&lmt=10"],
            'dogethb': [
                f"{rest_url}market/asks?sym={mapped_currency['dogethb']}&lmt=10",
                f"{rest_url}market/bids?sym={mapped_currency['dogethb']}&lmt=10"],
            'manathb': [
                f"{rest_url}market/asks?sym={mapped_currency['manathb']}&lmt=10",
                f"{rest_url}market/bids?sym={mapped_currency['manathb']}&lmt=10"],
            'usdtthb': [
                f"{rest_url}market/asks?sym={mapped_currency['usdtthb']}&lmt=10",
                f"{rest_url}market/bids?sym={mapped_currency['usdtthb']}&lmt=10"],
            'adathb': [
                f"{rest_url}market/asks?sym={mapped_currency['adathb']}&lmt=10",
                f"{rest_url}market/bids?sym={mapped_currency['adathb']}&lmt=10"],
            'sandthb': [
                f"{rest_url}market/asks?sym={mapped_currency['sandthb']}&lmt=10",
                f"{rest_url}market/bids?sym={mapped_currency['sandthb']}&lmt=10"],
            'dotthb': [
                f"{rest_url}market/asks?sym={mapped_currency['dotthb']}&lmt=10",
                f"{rest_url}market/bids?sym={mapped_currency['dotthb']}&lmt=10"],
            'sushithb': [
                f"{rest_url}market/asks?sym={mapped_currency['sushithb']}&lmt=10",
                f"{rest_url}market/bids?sym={mapped_currency['sushithb']}&lmt=10"],
            'galathb': [
                f"{rest_url}market/asks?sym={mapped_currency['galathb']}&lmt=10",
                f"{rest_url}market/bids?sym={mapped_currency['galathb']}&lmt=10"],
            'yfithb': [
                f"{rest_url}market/asks?sym={mapped_currency['yfithb']}&lmt=10",
                f"{rest_url}market/bids?sym={mapped_currency['yfithb']}&lmt=10"],
            'linkthb': [
                f"{rest_url}market/asks?sym={mapped_currency['linkthb']}&lmt=10",
                f"{rest_url}market/bids?sym={mapped_currency['linkthb']}&lmt=10"],
            'imxthb': [
                f"{rest_url}market/asks?sym={mapped_currency['imxthb']}&lmt=10",
                f"{rest_url}market/bids?sym={mapped_currency['imxthb']}&lmt=10"],
            'nearthb': [
                f"{rest_url}market/asks?sym={mapped_currency['nearthb']}&lmt=10",
                f"{rest_url}market/bids?sym={mapped_currency['nearthb']}&lmt=10"],
            'crvthb': [
                f"{rest_url}market/asks?sym={mapped_currency['crvthb']}&lmt=10",
                f"{rest_url}market/bids?sym={mapped_currency['crvthb']}&lmt=10"],
            'unithb': [
                f"{rest_url}market/asks?sym={mapped_currency['unithb']}&lmt=10",
                f"{rest_url}market/bids?sym={mapped_currency['unithb']}&lmt=10"]
        }

        urls = tuple(dict_values_getter(url_dict))
        while True:
            try:
                st = time.time()
                responses = await multiple_ulr_getter(session, urls)
                if responses[0]['error'] == 0:
                    before_db_save = time.time()
                    for i in range(len(responses) - 1):
                        cursor.execute(f"""INSERT INTO bitkub.{list(url_dict.keys())[int(i / 2)]}_ob (
                        ask_0, ask_vol_0, ask_1, ask_vol_1, ask_2, ask_vol_2, ask_3, ask_vol_3, ask_4, ask_vol_4,
                        ask_5, ask_vol_5, ask_6, ask_vol_6, ask_7, ask_vol_7, ask_8, ask_vol_8, ask_9, ask_vol_9,
                        bid_0, bid_vol_0, bid_1, bid_vol_1, bid_2, bid_vol_2, bid_3, bid_vol_3, bid_4, bid_vol_4,
                        bid_5, bid_vol_5, bid_6, bid_vol_6, bid_7, bid_vol_7, bid_8, bid_vol_8, bid_9, bid_vol_9, "timestamp")
                                                        VALUES (
                                                            {float(responses[i]['result'][0][3])},
                                                            {float(responses[i]['result'][0][4])},
                                                            {float(responses[i]['result'][1][3])},
                                                            {float(responses[i]['result'][1][4])},
                                                            {float(responses[i]['result'][2][3])},
                                                            {float(responses[i]['result'][2][4])},
                                                            {float(responses[i]['result'][3][3])},
                                                            {float(responses[i]['result'][3][4])},
                                                            {float(responses[i]['result'][4][3])},
                                                            {float(responses[i]['result'][4][4])},
                                                            {float(responses[i]['result'][5][3])},
                                                            {float(responses[i]['result'][5][4])},
                                                            {float(responses[i]['result'][6][3])},
                                                            {float(responses[i]['result'][6][4])},
                                                            {float(responses[i]['result'][7][3])},
                                                            {float(responses[i]['result'][7][4])},
                                                            {float(responses[i]['result'][8][3])},
                                                            {float(responses[i]['result'][8][4])},
                                                            {float(responses[i]['result'][9][3])},
                                                            {float(responses[i]['result'][9][4])},
                                                            {float(responses[i + 1]['result'][0][3])},
                                                            {float(responses[i + 1]['result'][0][4])},
                                                            {float(responses[i + 1]['result'][1][3])},
                                                            {float(responses[i + 1]['result'][1][4])},
                                                            {float(responses[i + 1]['result'][2][3])},
                                                            {float(responses[i + 1]['result'][2][4])},
                                                            {float(responses[i + 1]['result'][3][3])},
                                                            {float(responses[i + 1]['result'][3][4])},
                                                            {float(responses[i + 1]['result'][4][3])},
                                                            {float(responses[i + 1]['result'][4][4])},
                                                            {float(responses[i + 1]['result'][5][3])},
                                                            {float(responses[i + 1]['result'][5][4])},
                                                            {float(responses[i + 1]['result'][6][3])},
                                                            {float(responses[i + 1]['result'][6][4])},
                                                            {float(responses[i + 1]['result'][7][3])},
                                                            {float(responses[i + 1]['result'][7][4])},
                                                            {float(responses[i + 1]['result'][8][3])},
                                                            {float(responses[i + 1]['result'][8][4])},
                                                            {float(responses[i + 1]['result'][9][3])},
                                                            {float(responses[i + 1]['result'][9][4])},
                                                            {int(responses[i]['result'][0][1])});""")

                    logger.debug(f"Time of saving ob for {[*url_dict]}: {time.time() - before_db_save}")
                    logger.info(f"Ob received and successfully saved into database for {[*url_dict]} ")

                else:  # {"status": "Fail"} or other unexpected REST API responses
                    logger.error(
                        f" $$ Connection status: {str(responses[0]['error'])} if not 0 --> CHECK $$ ", exc_info=True)
                    time.sleep(5.0)

                await asyncio.sleep(5 - (time.time() - st))

            except (KeyError, RuntimeError, ContentTypeError, ClientOSError) as rest_error:
                logger.error(f" $$ {str(rest_error)} $$ ", exc_info=True)
                continue


if __name__ == '__main__':
    while True:
        try:
            asyncio.run(main())
        except (RuntimeError, KeyboardInterrupt, ClientConnectionError, ClientOSError, TimeoutError) as kill:
            logging_handler().error(f" $$ Connection kill, error: {str(kill)} $$ ", exc_info=True)
            continue
