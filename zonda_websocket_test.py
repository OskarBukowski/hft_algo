import asyncio
import websocket
import json
import ast
from websocket import create_connection
import logging
import datetime as dt
import sqlite3

##---------------------------
# KRAKEN
##---------------------------


# ws = create_connection("wss://ws.kraken.com/")
# ws.send('{"event":"subscribe", "subscription":{"name":"trade"}, "pair":["XBT/USD","XRP/USD"]}')
#
# while True:
#     print(ws.recv())


##---------------------------
# ZONDA
##---------------------------


conn = sqlite3.connect('orderbook_database.db')

logging.basicConfig(filename="logfile.log", level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

ws = create_connection("wss://api.zonda.exchange/websocket/")
ws.send('{"action": "subscribe-public","module": "trading","path": "orderbook/btc-pln"}')

while True:
    print(ws.recv())

    conn.execute(f'''
    INSERT INTO ZONDA_GLOBAL (symbol, side, rate, timestamp)
    VALUES (
    "{str(ast.literal_eval(ws.recv())['message']['changes'][0]['marketCode'])}",
    "{str(ast.literal_eval(ws.recv())['message']['changes'][0]['entryType'])}",
    {float(ast.literal_eval(ws.recv())['message']['changes'][0]['rate'])},
    {int(ast.literal_eval(ws.recv())['message']['timestamp'])}
    )
    '''
                 )
    conn.commit()

    logging.info(
        f"{dt.datetime.now()}; Ob received for {ast.literal_eval(ws.recv())['message']['changes'][0]['marketCode']}; timestamp: {ast.literal_eval(ws.recv())['message']['timestamp']}")

    # print(ast.literal_eval(ws.recv())['action'])  # library that converts the string repr of dict into real dict
