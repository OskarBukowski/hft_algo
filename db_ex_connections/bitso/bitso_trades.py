#!/usr/bin/env python3

import sys

sys.path.append("/home/obukowski/Desktop/hft_algo")

import websockets
from websockets import ConnectionClosedError, ConnectionClosedOK
import asyncio
from admin.admin_tools import logger_conf
import json
from json import JSONDecodeError
import psycopg2
import time




class Websocket:
    TRADES_COUNTER = 0
    # CURSOR = connection()
    LOGGER = logger_conf("../bitso/bitso.log")
    SUBSCRIPTIONS = {
        "xrpmxn": {"action": "subscribe", "book": "xrp_mxn", "type": "trades"},
        "btcmxn": {"action": "subscribe", "book": "btc_mxn", "type": "trades"},
        "ethmxn": {"action": "subscribe", "book": "eth_mxn", "type": "trades"},
        "manamxn": {"action": "subscribe", "book": "mana_mxn", "type": "trades"},
        "ltcusd": {"action": "subscribe", "book": "ltc_usd", "type": "trades"},
        "btcbrl": {"action": "subscribe", "book": 'btc_brl', "type": "trades"},
        "linkusd": {"action": "subscribe", "book": "link_usd", "type": "trades"},
        "batmxn": {"action": "subscribe", "book": "bat_mxn", "type": "trades"},
        "btcusd": {"action": "subscribe", "book": "btc_usd", "type": "trades"},
        "xrpusd": {"action": "subscribe", "book": "xrp_usd", "type": "trades"},
        "aaveusd": {"action": "subscribe", "book": "aave_usd", "type": "trades"},
        "ethbrl": {"action": "subscribe", "book": "eth_brl", "type": "trades"},
        "shibusd": {"action": "subscribe", "book": "shib_usd", "type": "trades"},
        "sandusd": {"action": "subscribe", "book": "sand_usd", "type": "trades"},
        "ftmusd": {"action": "subscribe", "book": "ftm_usd", "type": "trades"}
    }

    URL = "wss://ws.bitso.com"

    # def db_confirm(func):
    #     """Decorator that send confirmation if 20 trades's been saved correctly to postgres"""
    #     def confirm(self, *args):
    #         func(self, *args)
    #         if self.TRADES_COUNTER >= 20:
    #             self.LOGGER.info(f'Last {self.TRADES_COUNTER} trades saved correctly')
    #             self.TRADES_COUNTER = 0
    #     return confirm

    def open_connection(self):
        return websockets.connect(self.URL, ping_timeout=30, close_timeout=20)

    async def send_subscribe_message(self, websocket):
        for k, v in self.SUBSCRIPTIONS.items():
            await websocket.send(json.dumps(v))

    async def heartbeat_response_creator(self, response, session):
        """example heartbeat check: {"ping":1644437537572}"""
        await session.send('{{"pong": {}}}'.format(response['ping']))
        self.LOGGER.info("Heartbeat message sent with timestamp {}".format(response['ping']))

    def response_mapping(self, response_keys_tuple, response):
        """dict_keys(['id', 'status', 'subbed', 'ts'])
           dict_keys(['ch', 'ts', 'tick'])"""

        mapping_dict = {tuple(['id', 'status', 'subbed', 'ts']): self.subscription_confirmation}
                        # tuple(['ch', 'ts', 'tick']): self.database_save}

        return mapping_dict[response_keys_tuple](response)

    def subscription_confirmation(self, response):
        self.LOGGER.info(f"Trades stream subscription confirmed for {response['subbed'].split('.')[1]}")


    # def internal_database_save(self, response):
    #     for resp in response['tick']['data']:
    #         self.TRADES_COUNTER += 1
    #         st = time.time()
    #         self.CURSOR.execute(f"""INSERT INTO huobi.{response['ch'].split(".")[1]}_trades (id, price, volume, "timestamp")
    #                             VALUES (
    #                                     '{str(resp['tradeId'])}',
    #                                     {float(resp['price'])},
    #                                     {float(resp['amount'])},
    #                                     {int(resp['ts'])}
    #                                     );""")
    #
    #         self.LOGGER.debug(
    #             f"""Trade received on timestamp: {response['tick']['data'][0]['ts']} for {response['ch'].split(".")[1]},
    #             saving in database time: {time.time() - st}""")
    #
    # @db_confirm
    # def database_save(self, response):
    #     """{'ch': 'market.btcusdt.trade.detail',
    #         'ts': 1644438174613,
    #         'tick': {'id': 149028017931,
    #         'ts': 1644438174611,
    #         'data': [
    #                {'id': 149028017931472898124980116,
    #                 'ts': 1644438174611,
    #                 'tradeId': 102627793342,
    #                 'amount': 0.000330785964198955,
    #                 'price': 44466.86,
    #                 'direction': 'buy'}]}}"""
    #     try:
    #         self.internal_database_save(response)
    #
    #     except psycopg2.Error as database_saving_error:
    #         self.LOGGER.error(f" $$ {str(repr(database_saving_error))} $$ ", exc_info=True)
    #         time.sleep(10.0)

    def closed_connection(self, error_variable):
        self.LOGGER.debug(f" $$ {str(repr(error_variable))} $$ ", exc_info=True)
        self.LOGGER.warning("Received standard closing message")
        self.LOGGER.warning("Connection is closed, waiting 5sec to reconnect")
        time.sleep(5.0)

    def keyboard_interrupt(self, error_variable):
        self.LOGGER.debug(f" $$ {str(repr(error_variable))} $$ ", exc_info=True)
        self.LOGGER.warning("Received closing order")
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
                try:
                    if list(response.keys()) == ['ping']:
                        await socket_class.heartbeat_response_creator(response, wss)
                    else:
                        socket_class.response_mapping(tuple(list(response.keys())), response)

                except KeyError as unknown_message:
                    socket_class.LOGGER.error(f" $$ ----- {str(repr(unknown_message))} $$ ", exc_info=True)

        except (ConnectionClosedError, ConnectionClosedOK) as websocket_connection_error:
            socket_class.closed_connection(websocket_connection_error)
            continue

        except KeyboardInterrupt as stop_on_demand_error:
            socket_class.keyboard_interrupt(stop_on_demand_error)
            break


if __name__ == '__main__':
    asyncio.run(main())

