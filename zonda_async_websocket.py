import psycopg2
import websockets
import asyncio
import sqlite3
import ast
import logging
from admin_tools import connection

###
## TO DO :

# 1. logging is likely to be faster, need to add the logger to websocket.connect() configuration
# 2. check the pace of saving to database using sqlite and postgreSQL [ or other ]


# logging.basicConfig(filename="logfile.log", format="%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s", datefmt='%H:%M:%S')
# logger = logging.getLogger('wb_logger')
# logger.setLevel(logging.INFO)
#
#
# conn = sqlite3.connect('orderbook_database.db')
#
# async def main():
#     async with websockets.connect("wss://api.zonda.exchange/websocket/",
#                                   ping_timeout=30,
#                                   close_timeout=20) as websocket:
#
#         message_send = '{"action": "subscribe-public","module": "trading","path": "orderbook/btc-pln"}'
#         await websocket.send(message_send)
#         while True:
#             try:
#                 message_recv = await websocket.recv()
#                 dict_val = ast.literal_eval(message_recv)
#                 conn.execute(
#                     f'''INSERT INTO ZONDA_GLOBAL (symbol, side, rate, timestamp)
#                             VALUES (
#                                 "{str(dict_val['message']['changes'][0]['marketCode'])}",
#                                 "{str(dict_val['message']['changes'][0]['entryType'])}",
#                                 {float(dict_val['message']['changes'][0]['rate'])},
#                                 {int(dict_val['message']['timestamp'])}
#                         )'''
#                 )
#                 conn.commit()
#                 print(message_recv)
#                 logger.info(
#                     f"Ob received for {dict_val['message']['changes'][0]['marketCode']} timestamp: {dict_val['message']['timestamp']}")
#
#             except Exception as e: # there is the place for alert sended via messenger/slack/discord etc.
#                 print(e)
#
#
#
# asyncio.get_event_loop().run_until_complete(main())
# asyncio.get_event_loop().run_forever()  # it will skip the error and save ob without brakes

logging.basicConfig(filename="logfile.log", format="%(asctime)s.%(msecs)03d %(levelname)s  %(message)s",
                    datefmt='%H:%M:%S')
logger = logging.getLogger('wb_logger')
logger.setLevel(logging.INFO)
logger.setLevel(logging.ERROR)
logger.setLevel(logging.WARN)


async def main():
    cursor = connection().cursor()

    async with websockets.connect("wss://api.zonda.exchange/websocket/",
                                  ping_timeout=30,
                                  close_timeout=20) as websocket:

        message_send = '{"action": "subscribe-public","module": "trading","path": "orderbook/btc-pln"}'
        await websocket.send(message_send)
        while True:
            try:
                message_recv = await websocket.recv()
                print(message_recv)
                dict_val = ast.literal_eval(message_recv)

                try:
                    cursor.execute(
                        f'''INSERT INTO zonda.zonda_ob (symbol, side, rate, "timestamp")
                                VALUES (
                                    '{str(dict_val['message']['changes'][0]['marketCode'])}',
                                    '{str(dict_val['message']['changes'][0]['entryType'])}',
                                    {float(dict_val['message']['changes'][0]['rate'])},
                                    {int(dict_val['message']['timestamp'])}
                            )'''
                    )
                    connection().commit()

                except (Exception, psycopg2.Error) as db_error:
                    logger.error(
                        f"{db_error} while saving to database")

            except Exception as websocket_error:
                logger.error(
                    f"{websocket_error} :[ Ob {dict_val['message']['changes'][0]['marketCode']} ; {dict_val['message']['timestamp']} ]")

            logger.info(
                    f"Ob received for {dict_val['message']['changes'][0]['marketCode']} timestamp: {dict_val['message']['timestamp']}")


asyncio.get_event_loop().run_until_complete(main())
asyncio.get_event_loop().run_forever()  # it will skip the error and save ob without brakes
#
