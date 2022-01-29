#!/usr/bin/env python3


#####
# This script will use push in websocket API to get the update of last trades
# logs of this script will be saved into the same file like orderbooks to keep
# the clarity of data, and reduce the infrastructure complexity

### SCRIPT WORKS --> SAVES TO POSTGRES --> SAVES TO LOGFILE

import sys

sys.path.append("C:/Users/oskar/Desktop/hft_algo/hft_algo")

import websockets
import asyncio
from admin.admin_tools import connection, logger_conf
import json
import threading
import time


async def heartbeat_check(session, message):
    await session.send('{"action": "ping"}')
    response = await session.recv()
    if response == '{"action":"pong"}':
        await session.send(message)
    else:
        raise Exception('No heartbeat')




async def single_wss_run(message):
    cursor = connection()
    logger = logger_conf("../db_ex_connections/zonda.log")

    def timer():
        threading.Timer(30.0, timer).start()
        logger.info(f"Trades succesfully received on timestamp: {response['timestamp']}")

    async with websockets.connect("wss://api.zonda.exchange/websocket/",
                                  ping_timeout=30,
                                  close_timeout=20) as wss:
        await heartbeat_check(wss, message)
        while True:

            try:
                st = time.time()
                resp = await wss.recv()
                response = json.loads(resp)
                if response['action'] == "push":
                    print(response)
                    symbol = str(response['topic'].split('/')[2].replace("-", ""))
                    cursor.execute(f"""INSERT INTO zonda.{symbol}_trades (id, price, volume, "timestamp")
                                        VALUES (
                                                '{str(response['message']['transactions'][0]['id'])}',
                                                {float(response['message']['transactions'][0]['r'])},
                                                {float(response['message']['transactions'][0]['a'])},
                                                {int(response['timestamp'])}
                                                );""")

                    logger.debug(f"Trade received on timestamp: {response['timestamp']} for {symbol}")
                    timer()
                else:
                    continue

            except (Exception, websockets.ConnectionClosedOK, websockets.InvalidStatusCode) as websocket_error:
                logger.error(f" $$ {str(websocket_error)} $$ ", exc_info=True)


async def main():
    urls_dict = {
        'btcpln': '{"action": "subscribe-public","module": "trading","path": "transactions/btc-pln"}',
        'ethpln': '{"action": "subscribe-public","module": "trading","path": "transactions/eth-pln"}',
        'lunapln': '{"action": "subscribe-public","module": "trading","path": "transactions/luna-pln"}',
        'ftmpln': '{"action": "subscribe-public","module": "trading","path": "transactions/ftm-pln"}',
        'btceur': '{"action": "subscribe-public","module": "trading","path": "transactions/btc-eur"}',
        'xrppln': '{"action": "subscribe-public","module": "trading","path": "transactions/xrp-pln"}',
        'etheur': '{"action": "subscribe-public","module": "trading","path": "transactions/eth-eur"}',
        'adapln': '{"action": "subscribe-public","module": "trading","path": "transactions/ada-pln"}',
        'maticpln': '{"action": "subscribe-public","module": "trading","path": "transactions/matic-pln"}',
        'usdtpln': '{"action": "subscribe-public","module": "trading","path": "transactions/usdt-pln"}',
        'dotpln': '{"action": "subscribe-public","module": "trading","path": "transactions/dot-pln"}',
        'avaxpln': '{"action": "subscribe-public","module": "trading","path": "transactions/avax-pln"}',
        'dogepln': '{"action": "subscribe-public","module": "trading","path": "transactions/doge-pln"}',
        'trxpln': '{"action": "subscribe-public","module": "trading","path": "transactions/trx-pln"}',
        'manapln': '{"action": "subscribe-public","module": "trading","path": "transactions/mana-pln"}',
        'linkpln': '{"action": "subscribe-public","module": "trading","path": "transactions/link-pln"}'
    }

    all_connections = [single_wss_run(urls_dict[k]) for (k) in urls_dict.keys()]
    await asyncio.gather(*all_connections)


asyncio.run(main())
