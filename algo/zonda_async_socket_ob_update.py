#####
# This script gets the push response of orderbooks that will be needed for bot
# [ it is the update, the change of first line ],
# for analytical purposes, it is not necessary to get as complex data.


#####

### THIS SCRIPT WILL BE USED BY BOT TO GET THE MOST COMPREHENSIVE DATA

import websockets
import asyncio
import ast
from admin_tools.admin_tools import connection, logger_conf


###
## TO DO :

# 1. logging is likely to be faster, need to add the logger to websocket.connect() configuration
# it have to be inside the class with the logging configuration

async def main():
    cursor = connection()
    logger = logger_conf("../algo/zonda_ob.log")
    async with websockets.connect("wss://api.zonda.exchange/websocket/",
                                  ping_timeout=30,
                                  close_timeout=20) as websocket:

        message_send = '{"action": "subscribe-public","module": "trading","path": "orderbook/btc-pln"}'
        await websocket.send(message_send)
        while True:
            try:
                message_recv = await websocket.recv().json()
                cursor.execute(
                    f'''INSERT INTO zonda.zonda_ob (symbol, side, rate, "timestamp")
                                            VALUES (
                                                '{str(message_recv['message']['changes'][0]['marketCode'])}',
                                                '{str(message_recv['message']['changes'][0]['entryType'])}',
                                                {float(message_recv['message']['changes'][0]['rate'])},
                                                {int(message_recv['message']['timestamp'])}
                                        )'''
                )
                print(message_recv)
                logger.info(
                    f"Ob received for {message_recv['message']['changes'][0]['marketCode']} timestamp: {message_recv['message']['timestamp']}")

            except KeyError as websocket_error:
                logger.error(f" $$ {websocket_error} $$ ",
                                    exc_info=True)  # exc_info=True have to be added, cause without it the loop after error won't save logs any more


asyncio.get_event_loop().run_until_complete(main())
asyncio.get_event_loop().run_forever()  # it will skip the error and save ob without brake
