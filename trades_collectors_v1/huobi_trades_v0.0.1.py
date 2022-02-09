#!/usr/bin/env python3

import sys
sys.path.append("C:/Users/oskar/Desktop/hft_algo/hft_algo")

import websockets
from websockets import ConnectionClosedError, ConnectionClosedOK
import asyncio
import json
from json import JSONDecodeError
import time
from admin.admin_tools import connection, logger_conf
import gzip
import psycopg2



#
# async def main():
#
#     async with websockets.connect("wss://api.huobi.pro/ws",
#                                   ping_timeout=30,
#                                   close_timeout=20) as wss:
#
#
#         await wss.send('{"sub": "market.btcusdt.trade.detail","id": "id1"}')
#         while True:
#             resp = await wss.recv()
#             response = json.loads(gzip.decompress(resp).decode('utf-8'))
#             print(response)
#
# asyncio.run(main())



class Websocket:
    CURSOR = connection()
    LOGGER = logger_conf("../db_ex_connections/huobi.log")
    SUBSCRIPTIONS = {
        'btcusdt': '{"sub": "market.btcusdt.trade.detail","id": "id1"}',
        'ethusdt': '{"sub": "market.ethusdt.trade.detail","id": "id1"}',
        'shibusdt': '{"sub": "market.shibusdt.trade.detail","id": "id1"}',
        'avaxusdt': '{"sub": "market.avaxusdt.trade.detail","id": "id1"}',
        'filusdt': '{"sub": "market.filusdt.trade.detail","id": "id1"}',
        'adausdt': '{"sub": "market.adausdt.trade.detail","id": "id1"}',
        'solusdt': '{"sub": "market.solusdt.trade.detail","id": "id1"}',
        'xrpusdt': '{"sub": "market.xrpusdt.trade.detail","id": "id1"}',
        'trxusdt': '{"sub": "market.trxusdt.trade.detail","id": "id1"}',
        'galausdt': '{"sub": "market.galausdt.trade.detail","id": "id1"}',
        'manausdt': '{"sub": "market.manausdt.trade.detail","id": "id1"}',
        'dotusdt': '{"sub": "market.dotusdt.trade.detail","id": "id1"}',
        'lunausdt': '{"sub": "market.lunausdt.trade.detail","id": "id1"}',
        'sandusdt': '{"sub": "market.sandusdt.trade.detail","id": "id1"}',
        'dogeusdt': '{"sub": "market.dogeusdt.trade.detail","id": "id1"}',
        'axsusdt': '{"sub": "market.axsusdt.trade.detail","id": "id1"}',
        'maticusdt': '{"sub": "market.maticusdt.trade.detail","id": "id1"}'
    }

    URL = "wss://api.huobi.pro/ws"


    def timer(self):
        """Simple decorator that counts the time of execution"""
        def time_counter(*args):
            start = time.time()
            val = self(*args)
            print(f"Time of execution: {self.__name__}: {time.time() - start}")
            return val
        return time_counter()

    def open_connection(self):
        return websockets.connect(self.URL, ping_timeout=30, close_timeout=20)

    async def send_subscribe_message(self, websocket):
        for k, v in self.SUBSCRIPTIONS.items():
            await websocket.send(v)
            self.LOGGER.info(f"Subscribed for...{k}")

    async def heartbeat_response_creator(self, response, session):
        """example heartbeat check: {"ping":1644437537572}"""
        await session.send('{{"pong": {}}}'.format(response['ping']))
        self.LOGGER.info("Heartbeat message sent with timestamp {}".format(response['ping']))
        print('Pong message sebt')



    def perform_actions(self, response):
        """{'ch': 'market.btcusdt.trade.detail',
            'ts': 1644438174613,
            'tick': {'id': 149028017931,
            'ts': 1644438174611,
            'data': [
                   {'id': 149028017931472898124980116,
                    'ts': 1644438174611,
                    'tradeId': 102627793342,
                    'amount': 0.000330785964198955,
                    'price': 44466.86,
                    'direction': 'buy'}]}}"""
        try:
            for resp in response:
                st = time.time()
                self.CURSOR.execute(f"""INSERT INTO huobi.{response['ch'].split(".")[1]}_trades (id, price, volume, "timestamp")
                                    VALUES (
                                            '{str(resp['tick']['data'][0]['tradeId'])}',
                                            {float(resp['tick']['data'][0]['price'])},
                                            {float(resp['tick']['data'][0]['amount'])},
                                            {int(resp['tick']['data'][0]['ts'])}
                                            );""")

                self.LOGGER.info(f"Trade received for {response['ch'].split('.')[1]}")
                self.LOGGER.debug(
                    f"""Trade received on timestamp: {resp['tick']['data'][0]['ts']} for {response['ch'].split(".")[1]},
                    saving in database time: {time.time() - st}""")

        except psycopg2.Error as database_saving_error:
            self.LOGGER.error(f" $$ {str(repr(database_saving_error))} $$ ", exc_info=True)
            time.sleep(10.0)

        except KeyError as message_error:
            self.LOGGER.error(f" $$ {str(repr(message_error))} $$ ", exc_info=True)

        except (TypeError, JSONDecodeError) as received_message_error:
            self.LOGGER.error(f" $$ {str(repr(received_message_error))} $$ ", exc_info=True)
            time.sleep(10.0)

    def closed_connection(self, error_variable):
        self.LOGGER.debug(f" $$ {str(repr(error_variable))} $$ ", exc_info=True)
        self.LOGGER.warning("Received standard closing message")
        self.LOGGER.warning("Connection is closed, waiting 5sec to reconnect")
        time.sleep(5.0)

    def keyboard_interrupt(self):
        self.LOGGER.info("Closing application")


async def main():
    socket_class = Websocket()
    async for wss in socket_class.open_connection():
        try:
            await socket_class.send_subscribe_message(wss)
            while True:
                resp = await wss.recv()
                response = json.loads(gzip.decompress(resp).decode('utf-8'))
                print(response)
                if 'ping' in response:
                    print('true')
                    await socket_class.heartbeat_response_creator(response, wss)

                else:
                    print('false')
                    socket_class.perform_actions(response)

        except (ConnectionClosedError, ConnectionClosedOK) as websocket_connection_error:
            socket_class.closed_connection(websocket_connection_error)
            continue

        except KeyboardInterrupt as stop_on_demand_error:
            socket_class.closed_connection(stop_on_demand_error)
            break


if __name__ == '__main__':
    asyncio.run(main())





