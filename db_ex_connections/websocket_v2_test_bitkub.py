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
    LOGGER = logger_conf("../db_ex_connections/bitkub_2.log")
    MARKETS = ['market.trade.thb_btc',
               'market.trade.thb_eth',
               'market.trade.thb_doge',
               'market.trade.thb_usdt',
               'market.trade.thb_ada',
               'market.trade.thb_sand',
               'market.trade.thb_dot',
               'market.trade.thb_sushi',
               'market.trade.thb_gala',
               'market.trade.thb_yfi',
               'market.trade.thb_link',
               'market.trade.thb_imx',
               'market.trade.thb_mana']

    URL = "wss://api.bitkub.com/websocket-api/{},{},{},{},{},{},{},{},{},{},{},{},{}"

    def timer(self):
        """Simple decorator that counts the time of execution"""
        def time_counter(*args):
            start = time.time()
            val = self(*args)
            print(f"Time of execution: {self.__name__}: {time.time() - start}")
            return val
        return time_counter()

    def open_connection(self):
        return websockets.connect(self.URL.format(*self.MARKETS),
                                  ping_timeout=30,
                                  close_timeout=20)

    def perform_actions(self, responses_list):
        """expected response: {'amt': 9.01898734, 'bid': 4347240, 'rat': 11.26, 'sid': 4098837,
        'stream': 'market.trade.thb_gala', 'sym': 'THB_GALA', 'ts': 1644157630, 'txn': 'GALASELL0001325407'}"""
        try:

            for response in responses_list:
                response = json.loads(response)
                print(response)
                try:
                    st = time.time()
                    self.CURSOR.execute(
                        f"""INSERT INTO bitkub.{response['stream'].split('_')[1]}thb_trades (id, price, volume, "timestamp")
                                                VALUES (
                                                         '{str(response['txn'])}',
                                                         {float(response['rat'])},
                                                         {float(response['amt'])},
                                                         {int(response['ts'] * 1000)});""")

                    self.LOGGER.info(f"Trade received for {response['stream'].split('_')[1]}")
                    self.LOGGER.debug(
                        f"Trade received on timestamp: {int(response['ts'] * 1000)} for {response['stream'].split('_')[1]}")
                    self.LOGGER.debug(
                        f"Saving in database time: {time.time() - st} for {response['stream'].split('_')[1]}")

                except psycopg2.Error as database_saving_error:
                    self.LOGGER.error(f" $$ {str(repr(database_saving_error))} $$ ", exc_info=True)
                    time.sleep(10.0)

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
            while True:
                resp = await wss.recv()
                response = json.loads(json.dumps(resp.split("\n")))
                socket_class.perform_actions(response)

        except (ConnectionClosedError, ConnectionClosedOK) as websocket_connetion_error:
            socket_class.closed_connection(websocket_connetion_error)
            continue

        except KeyboardInterrupt as stop_on_demand_error:
            socket_class.closed_connection(stop_on_demand_error)
            break


if __name__ == '__main__':
    asyncio.run(main())
