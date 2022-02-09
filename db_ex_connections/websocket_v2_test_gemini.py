#!/usr/bin/env python3

import sys
sys.path.append("C:/Users/oskar/Desktop/hft_algo/hft_algo")
# sys.path.append("/home/obukowski/Desktop/repo/hft_algo")

import websockets
from websockets import ConnectionClosedError, ConnectionClosedOK
import asyncio
from admin.admin_tools import connection, logger_conf
import json
from json import JSONDecodeError
import time
import psycopg2


class Websocket:
    CURSOR = connection()
    LOGGER = logger_conf("../db_ex_connections/gemini.log")
    SUBSCRIPTIONS = ["btcusd", "ethusd", "dogeusd", "maticusd", "sushiusd", "ftmusd" ,"linkusd", "sandusd",
                     "filusd", "galausd", "manausd", "lrcusd", "lunausd", "crvusd", "aaveusd", "uniusd", "axsusd"]

    URL = "wss://api.gemini.com/v1/marketdata/{}?trades=true"


    def timer(self):
        """Simple decorator that counts the time of execution"""
        def time_counter(*args):
            start = time.time()
            val = self(*args)
            print(f"Time of execution: {self.__name__}: {time.time() - start}")
            return val
        return time_counter()

    def open_connection(self, ticker):
        return websockets.connect(self.URL.format(ticker), ping_timeout=30, close_timeout=20)

    async def single_wss_run(self, ticker):
        async for wss in self.open_connection(ticker):
            try:
                while True:
                    resp = await wss.recv()
                    response = json.loads(resp)
                    print(response)
                    self.perform_actions(response, ticker)
            except (ConnectionClosedError, ConnectionClosedOK) as websocket_connection_error:
                self.closed_connection(websocket_connection_error)
                continue
            except KeyboardInterrupt as stop_on_demand_error:
                self.closed_connection(stop_on_demand_error)
                break

    def perform_actions(self, response, ticker):
        """expected response: {'type': 'update','eventId': 87313124465,'timestamp': 1644296532,
        'timestampms': 1644296532720,'socket_sequence': 2,'events': [{
                                                                     'type': 'trade',
                                                                     'tid': 87313124465,
                                                                     'price': '44365.23',
                                                                     'amount': '0.00353116',
                                                                     'makerSide': 'bid'}]}"""
        try:
            if response['socket_sequence'] > 0:
                try:
                    st = time.time()
                    self.CURSOR.execute(f"""INSERT INTO gemini.{ticker}_trades 
                                        (id, price, volume, "timestamp")
                                        VALUES (
                                                '{str(response['eventId'])}',
                                                {float(response['events'][0]['price'])},
                                                {float(response['events'][0]['amount'])},
                                                {int(response['timestampms'])});""")

                    self.LOGGER.info(f"Trade received for {ticker}")
                    self.LOGGER.debug(
                        f"""Trade received on timestamp: {response['timestampms']} for {ticker}"),
                    saving in database time: {time.time() - st}""")

                except psycopg2.Error as database_saving_error:
                    self.LOGGER.error(f" $$ {str(repr(database_saving_error))} $$ ", exc_info=True)
                    time.sleep(10.0)
            else:
                try:
                    self.LOGGER.info(f"Starting ..., code: {response['socket_sequence']} for {ticker}")
                except KeyError:
                    try:
                        self.LOGGER.error(f"$$ Code: {response['result']} $$")
                        self.LOGGER.error(f"$$ Code: {response['reason']} $$")
                        self.LOGGER.error(f"$$ Code: {response['message']} $$")
                    except KeyError as websocket_error:
                        self.LOGGER.error(f"{str(websocket_error)}")

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
    all_connections = [socket_class.single_wss_run(i) for i in socket_class.SUBSCRIPTIONS]
    await asyncio.gather(*all_connections)



if __name__ == '__main__':
    asyncio.run(main())
