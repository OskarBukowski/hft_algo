#!/usr/bin/env python3


import sys
sys.path.append("C:/Users/oskar/Desktop/hft_algo/hft_algo")
sys.path.append("/home/obukowski/Desktop/repo/hft_algo")

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
    LOGGER = logger_conf("../db_ex_connections/wazirx.log")
    SUBSCRIPTIONS = [
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

    URL = "wss://stream.wazirx.com/stream"


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
        for s in self.SUBSCRIPTIONS:
            await websocket.send(s)



    def perform_actions(self, response):
        """expected response: {'data': {'trades': [{'E': 1644209126000, 'S': 'buy',
                                'a': 2279524079, 'b': 2279700645, 'm': True, 'p': '90.7004',
                                'q': '10.0', 's': 'adainr', 't': 340438749}]}, 'stream': 'adainr@trades'}"""
        try:
            if list(response.keys()) == ['data', 'stream']:
                try:
                    st = time.time()
                    self.CURSOR.execute(f"""INSERT INTO wazirx.{response['data']['trades'][0]['s']}_trades 
                                        (id, price, volume, "timestamp")
                                        VALUES (
                                                '{str(response['data']['trades'][0]['a'])}',
                                                {float(response['data']['trades'][0]['p'])},
                                                {float(response['data']['trades'][0]['q'])},
                                                {int(response['data']['trades'][0]['E'])});""")

                    self.LOGGER.info(f"Trade received for {response['data']['trades'][0]['s']}")
                    self.LOGGER.debug(
                        f"Trade received on timestamp: {response['data']['trades'][0]['E']} for {response['data']['trades'][0]['s']}")
                    self.LOGGER.debug(
                        f"Saving in database time: {time.time() - st} for {response['data']['trades'][0]['s']}")

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
                response = json.loads(resp)
                socket_class.perform_actions(response)

        except (ConnectionClosedError, ConnectionClosedOK) as websocket_connection_error:
            socket_class.closed_connection(websocket_connection_error)
            continue

        except KeyboardInterrupt as stop_on_demand_error:
            socket_class.closed_connection(stop_on_demand_error)
            break


if __name__ == '__main__':
    asyncio.run(main())
