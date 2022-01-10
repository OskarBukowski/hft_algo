import ast
from websocket import create_connection
import logging
import datetime as dt
import sqlite3








logging.basicConfig(filename="logfile.log", format="%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s", datefmt='%H:%M:%S')

logger = logging.getLogger()

logger.setLevel(logging.INFO)



conn = sqlite3.connect('orderbook_database.db')
ws = create_connection("wss://api.zonda.exchange/websocket/")
ws.send('{"action": "subscribe-public","module": "trading","path": "orderbook/btc-pln"}')

while True:
    print(ws.recv())  # delete in production, because it also makes it slower

    dict_val = ast.literal_eval(ws.recv())

    conn.execute(
        f'''INSERT INTO ZONDA_GLOBAL (symbol, side, rate, timestamp)
            VALUES (
                "{str(dict_val['message']['changes'][0]['marketCode'])}",
                "{str(dict_val['message']['changes'][0]['entryType'])}",
                {float(dict_val['message']['changes'][0]['rate'])},
                {int(dict_val['message']['timestamp'])}
        )'''
                 )
    conn.commit()

    logger.info(
        f"Ob received for {dict_val['message']['changes'][0]['marketCode']} | timestamp: {dict_val['message']['timestamp']}")


