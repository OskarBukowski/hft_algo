#!/usr/bin/env python3

import sys
sys.path.append("C:/Users/oskar/Desktop/hft_algo/hft_algo")

# There is an issue that json cannot load multiple websocket responses

import websockets
import asyncio
from admin.admin_tools import connection, logger_conf
import json
from json.decoder import JSONDecodeError


async def single_wss_run(socket):
    cursor = connection()
    logger = logger_conf("../db_ex_connections/bitkub.log")

    async with websockets.connect(socket,
                                  ping_timeout=30,
                                  close_timeout=20) as wss:

        while True:
            try:
                response = await wss.recv()
                resp = json.loads(json.dumps(response))

                cursor.execute(
                    f"""INSERT INTO bitkub.{resp['stream'].split('_')[1]}thb_trades (id, price, volume, "timestamp")
                        VALUES (
                                 '{str(resp['txn'])}',
                                 {float(resp['rat'])},
                                 {float(resp['amt'])},
                                 {int(resp['ts'] * 1000)});""")

            except (Exception,
                    JSONDecodeError,
                    websockets.ConnectionClosedOK,
                    websockets.InvalidStatusCode) as websocket_error:

                logger.error(f" $$ {str(repr(websocket_error))} $$ ", exc_info=True)




async def main():
    # url_dict = {
    #     'btcthb': 'wss://api.bitkub.com/websocket-api/market.trade.thb_btc',
    #     'eththb': 'wss://api.bitkub.com/websocket-api/market.trade.thb_eth',
    #     'dogethb': 'wss://api.bitkub.com/websocket-api/market.trade.thb_doge',
    #     'usdtthb': 'wss://api.bitkub.com/websocket-api/market.trade.thb_usdt',
    #     'adathb': 'wss://api.bitkub.com/websocket-api/market.trade.thb_ada',
    #     'sandthb': 'wss://api.bitkub.com/websocket-api/market.trade.thb_sand',
    #     'dotthb': 'wss://api.bitkub.com/websocket-api/market.trade.thb_dot',
    #     'sushithb': 'wss://api.bitkub.com/websocket-api/market.trade.thb_sushi',
    #     'galathb': 'wss://api.bitkub.com/websocket-api/market.trade.thb_gala',
    #     'yfithb': 'wss://api.bitkub.com/websocket-api/market.trade.thb_yfi',
    #     'linkthb': 'wss://api.bitkub.com/websocket-api/market.trade.thb_link',
    #     'imxthb': 'wss://api.bitkub.com/websocket-api/market.trade.thb_imx',
    #     'manathb': 'wss://api.bitkub.com/websocket-api/market.trade.thb_mana'}

    markets = ['market.trade.thb_btc',
               'market.trade.thb_eth',
               'market.trade.thb_doge',
               'market.trade.thb_usdt',
               'market.trade.thb_ada',
               'market.trade.thb_sand',
               'market.trade.thb_dot',
               'market.trade.thb_sushi',
               'market.trade.thb_gala',
               'market.trade.thb_yfi',
               'market.trade.thb_link',
               'market.trade.thb_imx',
               'market.trade.thb_mana']

    url = "wss://api.bitkub.com/websocket-api/{},{},{},{},{},{},{},{},{},{},{},{},{}"
    all_connections = [single_wss_run(url.format(*markets))]
    await asyncio.gather(*all_connections)


asyncio.run(main())
