#!/usr/bin/env python3

import sys
sys.path.append("C:/Users/oskar/Desktop/hft_algo/hft_algo")

import websockets
import asyncio
from admin.admin_tools import connection, logger_conf
import json
import time


async def request_handler(requests: list, connection):
    for i in requests:
        await connection.send(i)


async def main():
    cursor = connection()
    logger = logger_conf("../db_ex_connections/wazirx.log")

    async with websockets.connect("wss://stream.wazirx.com/stream",
                                  ping_timeout=30,
                                  close_timeout=20) as wss:
        requests = [
            '''{"event":"subscribe",
                            "streams":
                            ["btcinr@trades","ethinr@trades","dogeinr@trades","maticinr@trades","adainr@trades"]}''',
            '''{"event":"subscribe",
                            "streams":
                            ["ftminr@trades","xrpinr@trades","sandinr@trades","usdtinr@trades","solinr@trades"]}''',
            '''{"event":"subscribe",
                            "streams":
                            ["manainr@trades","dotinr@trades","lunainr@trades","trxinr@trades","vetinr@trades"]}''',
            '''{"event":"subscribe",
                            "streams":
                            ["lunausdt@trades","ethusdt@trades"]}''']

        await request_handler(requests, wss)

        while True:
            try:
                st = time.time()
                resp = await wss.recv()
                response = json.loads(resp)
                print(response)
                if list(response.keys()) == ['data', 'stream']:
                    cursor.execute(f"""INSERT INTO wazirx.{response['data']['trades'][0]['s']}_trades (id, price, volume, "timestamp")
                                        VALUES (
                                                '{str(response['data']['trades'][0]['a'])}',
                                                {float(response['data']['trades'][0]['p'])},
                                                {float(response['data']['trades'][0]['q'])},
                                                {int(response['data']['trades'][0]['E'])}
                                                );""")
                    logger.info(f"Trade received for {response['data']['trades'][0]['s']}")
                    logger.debug(
                        f"Trade received on timestamp: {response['data']['trades'][0]['E']} for {response['data']['trades'][0]['s']}")
                    logger.debug(
                        f"Saving in database time: {time.time() - st} for {response['data']['trades'][0]['s']}")

                else:
                    try:
                        logger.error(f"$$ Code: {response['data']['code']}, message: {response['data']['message']} $$")
                    except KeyError:
                        logger.info(f"Starting ... {str(response['data'])}")

            except (Exception, websockets.ConnectionClosedOK, websockets.InvalidStatusCode) as websocket_error:
                logger.error(f" $$ {str(websocket_error)} $$ ", exc_info=True)


asyncio.run(main())
