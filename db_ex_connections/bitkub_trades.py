#!/usr/bin/env python3

#To do:

#1. Add safety connection close

# from logs: websockets.exceptions.ConnectionClosedError

import sys

sys.path.append("C:/Users/oskar/Desktop/hft_algo/hft_algo")

import websockets
import asyncio
import json
import time
from json.decoder import JSONDecodeError
from admin.admin_tools import connection, logger_conf


async def main():
    cursor = connection()
    logger = logger_conf("../db_ex_connections/bitkub.log")

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

    async with websockets.connect(url.format(*markets),
                                  ping_timeout=30,
                                  close_timeout=20) as wss:

        while True:
            try:
                st = time.time()
                resp = await wss.recv()
                response = json.loads(json.dumps(resp.split("\n")))

                for r in response:
                    r = json.loads(r)
                    print(r)
                    cursor.execute(
                        f"""INSERT INTO bitkub.{r['stream'].split('_')[1]}thb_trades (id, price, volume, "timestamp")
                            VALUES (
                                     '{str(r['txn'])}',
                                     {float(r['rat'])},
                                     {float(r['amt'])},
                                     {int(r['ts'] * 1000)});""")

                    logger.info(f"Trade received for {r['stream'].split('_')[1]}")

                    logger.debug(
                        f"Trade received on timestamp: {int(r['ts'] * 1000)} for {r['stream'].split('_')[1]}")
                    logger.debug(f"Saving in database time: {time.time() - st} for {r['stream'].split('_')[1]}")

            except (Exception,
                    TypeError,
                    JSONDecodeError,
                    websockets.ConnectionClosedOK,
                    websockets.InvalidStatusCode) as websocket_error:

                logger.error(f" $$ {str(repr(websocket_error))} $$ ", exc_info=True)


asyncio.run(main())
