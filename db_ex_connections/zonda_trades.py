#!/usr/bin/env python3


#####
# This script will use push in websocket API to get the update of last trades
# logs of this script will be saved into the same file like orderbooks to keep
# the clarity of data, and reduce the infrastructure complexity

### SCRIPT WORKS --> SAVES TO POSTGRES --> SAVES TO LOGFILE


import websockets
import asyncio
from admin_tools.admin_tools import connection, logger_conf
import json





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

    async with websockets.connect("wss://api.zonda.exchange/websocket/",
                                  ping_timeout=30,
                                  close_timeout=20) as wss:
        await heartbeat_check(wss, message)
        while True:
            try:
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

                    logger.info(f"Trades received on timestamp: {response['timestamp']}")
                else:
                    continue

            except (Exception, websockets.ConnectionClosedOK, websockets.InvalidStatusCode) as websocket_error:
                logger.error(f" $$ {str(websocket_error)} $$ ", exc_info=True)


async def main():
    messages_dict = {
        'btcpln': '{"action": "subscribe-public","module": "trading","path": "transactions/btc-pln"}',
        'ethpln': '{"action": "subscribe-public","module": "trading","path": "transactions/eth-pln"}',
        'lunapln': '{"action": "subscribe-public","module": "trading","path": "transactions/luna-pln"}',
        'adapln': '{"action": "subscribe-public","module": "trading","path": "transactions/ada-pln"}',
        'xrppln': '{"action": "subscribe-public","module": "trading","path": "transactions/xrp-pln"}',
        'maticpln': '{"action": "subscribe-public","module": "trading","path": "transactions/matic-pln"}',
        'ftmpln': '{"action": "subscribe-public","module": "trading","path": "transactions/ftm-pln"}',

    }

    all_connections = [single_wss_run(messages_dict[k]) for (k) in messages_dict.keys()]
    await asyncio.gather(*all_connections)


asyncio.run(main())
