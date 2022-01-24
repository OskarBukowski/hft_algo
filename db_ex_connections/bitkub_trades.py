#!/usr/bin/env python3


import websockets
import asyncio
from admin_tools.admin_tools import connection, logger_conf
import json


# async def single_wss_run(socket, message):
#     cursor = connection()
#     logger = logger_conf("../db_ex_connections/bitkub.log")
#
#     async with websockets.connect(socket,
#                                   ping_timeout=30,
#                                   close_timeout=20) as wss:
#
#         while True:
#             try:
#                 resp = await wss.recv()
#                 response = json.loads(resp)
#                 if response['action'] == "push":
#                     cursor.execute(f"""INSERT INTO bitkub.{symbol}_trades (id, price, volume, "timestamp")
#                                         VALUES (
#                                                 '{str(response['message']['transactions'][0]['id'])}',
#                                                 {float(response['message']['transactions'][0]['r'])},
#                                                 {float(response['message']['transactions'][0]['a'])},
#                                                 {int(response['timestamp'])}
#                                                 );""")
#
#                     logger.info(f"Trades received on timestamp: {response['timestamp']}")
#                 else:
#                     continue
#
#             except (Exception, websockets.ConnectionClosedOK, websockets.InvalidStatusCode) as websocket_error:
#                 logger.error(f" $$ {str(websocket_error)} $$ ", exc_info=True)
#


async def single_wss_run(socket):
    cursor = connection()
    logger = logger_conf("../db_ex_connections/bitkub.log")

    async with websockets.connect(socket,
                                  ping_timeout=30,
                                  close_timeout=20) as wss:

        while True:
            try:
                resp = await wss.recv()
                response = json.loads(resp)
                print(response)

                cursor.execute(
                    f"""INSERT INTO bitkub.{response['stream'].split('_')[1]}thb_trades (id, price, volume, "timestamp")
                        VALUES (
                                 '{str(response['txn'])}',
                                 {float(response['rat'])},
                                 {float(response['amt'])},
                                 {int(response['ts']*1000)});""")


            except (Exception, websockets.ConnectionClosedOK, websockets.InvalidStatusCode) as websocket_error:
                logger.error(f" $$ {str(websocket_error)} $$ ", exc_info=True)


async def main():
    url_dict = {
        'btcthb': 'wss://api.bitkub.com/websocket-api/market.trade.thb_btc',
        'eththb': 'wss://api.bitkub.com/websocket-api/market.trade.thb_eth',
        'dogethb': 'wss://api.bitkub.com/websocket-api/market.trade.thb_doge',
        'usdtthb': 'wss://api.bitkub.com/websocket-api/market.trade.thb_usdt',
        'adathb': 'wss://api.bitkub.com/websocket-api/market.trade.thb_ada',
        'sandthb': 'wss://api.bitkub.com/websocket-api/market.trade.thb_sand',
        'dotthb': 'wss://api.bitkub.com/websocket-api/market.trade.thb_dot',
        'sushithb': 'wss://api.bitkub.com/websocket-api/market.trade.thb_sushi',
        'galathb': 'wss://api.bitkub.com/websocket-api/market.trade.thb_gala',
        'yfithb': 'wss://api.bitkub.com/websocket-api/market.trade.thb_yfi',
        'linkthb': 'wss://api.bitkub.com/websocket-api/market.trade.thb_link',
        'imxthb': 'wss://api.bitkub.com/websocket-api/market.trade.thb_imx',
        'manathb': 'wss://api.bitkub.com/websocket-api/market.trade.thb_mana'}

    all_connections = [single_wss_run(url_dict[k]) for (k) in url_dict.keys()]
    await asyncio.gather(*all_connections)


asyncio.run(main())
