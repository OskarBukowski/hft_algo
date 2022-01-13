#####
# This script gets the push response of orderbooks that will be needed for bot
# [ it is the update, the change of first line ],
# for analytical purposes, it is not necessary to get as complex data.


#####

### THIS SCRIPT WILL BE USED BY BOT TO GET THE MOST COMPREHENSIVE DATA

import websockets
import asyncio
import ast
from admin_tools import connection, logger_conf


###
## TO DO :

# 1. logging is likely to be faster, need to add the logger to websocket.connect() configuration
# it have to be inside the class with the logging configuration

async def main():
    cursor = connection()
    async with websockets.connect("wss://api.zonda.exchange/websocket/",
                                  ping_timeout=30,
                                  close_timeout=20) as websocket:

        message_send = '{"action": "subscribe-public","module": "trading","path": "orderbook/btc-pln"}'
        await websocket.send(message_send)
        while True:
            try:
                message_recv = await websocket.recv()
                dict_val = ast.literal_eval(message_recv)
                cursor.execute(
                    f'''INSERT INTO zonda.zonda_ob (symbol, side, rate, "timestamp")
                                            VALUES (
                                                '{str(dict_val['message']['changes'][0]['marketCode'])}',
                                                '{str(dict_val['message']['changes'][0]['entryType'])}',
                                                {float(dict_val['message']['changes'][0]['rate'])},
                                                {int(dict_val['message']['timestamp'])}
                                        )'''
                )
                print(message_recv)
                logger_conf().info(
                    f"Ob received for {dict_val['message']['changes'][0]['marketCode']} timestamp: {dict_val['message']['timestamp']}")

            except KeyError as websocket_error:
                logger_conf().error(f" $$ {websocket_error} $$ ",
                                    exc_info=True)  # exc_info=True have to be added, cause without it the loop after error won't save logs any more


asyncio.get_event_loop().run_until_complete(main())
asyncio.get_event_loop().run_forever()  # it will skip the error and save ob without brake
