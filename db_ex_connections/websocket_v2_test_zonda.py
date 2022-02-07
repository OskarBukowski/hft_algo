#!/usr/bin/env python3


import sys
sys.path.append("C:/Users/oskar/Desktop/hft_algo/hft_algo")
# sys.path.append("/home/obukowski/Desktop/repo/hft_algo/hft_algo")

import websockets
from websockets import ConnectionClosedError, ConnectionClosedOK
import asyncio
import json
import psycopg2
import time
from json.decoder import JSONDecodeError
from admin.admin_tools import connection, logger_conf


class Websocket:
    CURSOR = connection()
    LOGGER = logger_conf("../db_ex_connections/zonda_2.log")
    SUBSCRIPTIONS = {
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

    URL = "wss://api.zonda.exchange/websocket/"


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

    def heartbeat_check(self, session):
        await session.send('{"action": "ping"}')
        response = await session.recv()
        if response == '{"action":"pong"}':
            await self.send_subscribe_message(session)
        else:
            self.LOGGER.info(f"No heartbeat, check the zondaglobal.com API details")
            self.LOGGER.info(f"Check maintenance")



    def perform_actions(self, response):
        """expected response: {'action': 'push', 'topic': 'trading/transactions/usdt-pln',
                                'message': {'transactions': [{'id': 'b3379207-87d5-11ec-ab8e-0242ac11000f',
                                't': '1644211244619', 'a': '14.25', 'r': '4', 'ty': 'Buy'}]},
                                'timestamp': '1644211244619', 'seqNo': 467515}"""
        try:
            if list(response.keys()) == ['data', 'stream']:
                try:
                    st = time.time()
                    if response['action'] == "push":
                        symbol = str(response['topic'].split('/')[2].replace("-", ""))
                        self.CURSOR.execute(f"""INSERT INTO zonda.{symbol}_trades (id, price, volume, "timestamp")
                                            VALUES (
                                                    '{str(response['message']['transactions'][0]['id'])}',
                                                    {float(response['message']['transactions'][0]['r'])},
                                                    {float(response['message']['transactions'][0]['a'])},
                                                    {int(response['timestamp'])}
                                                    );""")

                    self.LOGGER.info(f"Trade received for {symbol}")
                    self.LOGGER.debug(
                        f"Trade received on timestamp: {response['data']['trades'][0]['E']} for {symbol}")
                    self.LOGGER.debug(
                        f"Saving in database time: {time.time() - st} for {symbol}")

                except psycopg2.Error as database_saving_error:
                    self.LOGGER.error(f" $$ {str(repr(database_saving_error))} $$ ", exc_info=True)
                    time.sleep(10.0)
            else:
                try:
                    self.LOGGER.error(f"$$ Code: {response['data']['code']} $$")
                except KeyError:
                    try:
                        self.LOGGER.info(f"Starting ... {str(response['data'])}")
                    except KeyError:
                        self.LOGGER.info(f"Connection closed message: {str(response['message'])}")

        except (TypeError, JSONDecodeError) as received_message_error:
            self.LOGGER.error(f" $$ {str(repr(received_message_error))} $$ ", exc_info=True)
            time.sleep(10.0)

    def closed_connection(self, error_variable):
        self.LOGGER.error(f" $$ {str(repr(error_variable))} $$ ", exc_info=True)
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
                response = json.loads(resp)
                print(response)
                socket_class.perform_actions(response)

        except (ConnectionClosedError, ConnectionClosedOK) as websocket_connetion_error:
            socket_class.closed_connection(websocket_connetion_error)
            continue

        except KeyboardInterrupt as stop_on_demand_error:
            socket_class.closed_connection(stop_on_demand_error)
            break


if __name__ == '__main__':
    asyncio.run(main())
