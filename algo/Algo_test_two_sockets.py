import requests
import websocket
import threading
import numpy as np
import json
from datetime import datetime
import gzip
import requests
from datetime import datetime
import time
from admin.admin_tools import logger_conf
from multiprocessing import Process


# TO DO:
# 1. Create classes for two instruments with inheritance from Client class
# 2. Modify Client() class to have stable response for given events, deal with WebsocketApp() class response
# 3. Check the lock() option to get synchronized responses


class Client(threading.Thread):
    LOGGER = logger_conf("../algo/two_socket_test.log")

    def __init__(self, url, exchange):
        super().__init__()
        self.ws = websocket.WebSocketApp(
            url=url,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open)

        self.exchange = exchange

    def run(self, *args):
        while True:
            self.ws.run_forever()

    def on_message(self, *args):
        pass

    def on_error(self, *args):
        print('Error appeared')

    def on_close(self, *args):
        print("### closed ###", args)

    def on_open(self, *args):
        """There we need only one argument, because the whole response is <websocket._app.WebSocketApp object>"""
        self.LOGGER.info(f'Subscribed for {self.exchange}')


class Zonda(Client):
    LOGGER = logger_conf("../algo/two_socket_test.log")
    PUSH_COUNTER = 0

    def __init__(self, url, exchange, orderbook_handler, lock):
        super().__init__(url, exchange)

        self.url = url
        self.exchange = exchange
        self.orderbook_handler = orderbook_handler
        self.lock = lock

    def heartbeat(func):
        """Decorator that send heartbeat message every 20 push received"""
        def heartbeat_sender(self, *args):
            func(self, *args)
            if self.PUSH_COUNTER == 20:
                self.ws.send('{"action": "ping"}')
                self.PUSH_COUNTER = 0
        return heartbeat_sender

    @property
    def name(self):
        return type(self).__name__


    def heartbeat_confirm(self, response):
        self.LOGGER.info(f'Zonda: heartbeat message confirmed with {response["action"]} ; received on {round(int(time.time()) * 1000)}')


    def on_open(self, *args):
        super().on_open()
        self._snapshot()
        self.ws.send('{"action": "subscribe-public","module": "trading","path": "orderbook-limited/btc-pln/10"}')

    def _snapshot(self):
        return self.ws.send(
            '{"requestId": "78539fe0-e9b0-4e4e-8c86-70b36aa93d4f","action": "proxy","module": "trading",'
            '"path": "orderbook-limited/btc-pln/10"}')

    def on_message(self, object, response):
        response = json.loads(response)
        print(response)
        try:
            self._response_mapping(tuple(list(response.keys())), response)
        except KeyError as unknown_response_type:
            self.LOGGER.info(f'Unknown response: {unknown_response_type}')

    def snapshot_handler(self, response):
        with self.lock:
            self.orderbook_handler[self.name][0] = [float(response['body']['sell'][0][i]) for i in ['ra', 'ca']]
            self.orderbook_handler[self.name][1] = [float(response['body']['buy'][-1][i]) for i in ['ra', 'ca']]

    @heartbeat
    def push_handler(self, response):
            self.PUSH_COUNTER += 1
            self.LOGGER.info(self.PUSH_COUNTER)
            for r in response['message']['changes']:
                if r['rate'] == self._check_price_match[r['entryType']]:
                    self.LOGGER.info(r['rate'])
                    self.LOGGER.info(self._check_price_match[r['entryType']])
                    self._check_price_match[r['entryType']] = [r['state'][i] for i in ['ra', 'ca']]

    @property
    def _check_price_match(self):
        return {'Buy': self.orderbook_handler[self.name][1][0], 'Sell': self.orderbook_handler[self.name][0][0]}

    def _response_mapping(self, response_keys_tuple, response):
        mapping_dict = {tuple(['action', 'requestId', 'statusCode', 'body']): self.snapshot_handler,
                        tuple(['action', 'topic', 'message', 'timestamp', 'seqNo']): self.push_handler,
                        tuple(['action', 'module', 'path']): self.subscription_confirm,
                        tuple(['action']): self.heartbeat_confirm}

        return mapping_dict[response_keys_tuple](response)

    def subscription_confirm(self, response):
        self.LOGGER.info(f'Subscription confirmed for {response["path"].split("/")[1]}')


class Huobi(Client):
    LOGGER = logger_conf("../algo/two_socket_test.log")

    def __init__(self, url, exchange, orderbook_handler, lock):
        super().__init__(url, exchange)

        self.url = url
        self.exchange = exchange
        self.orderbook_handler = orderbook_handler
        self.lock = lock

    @property
    def name(self):
        return type(self).__name__

    def on_open(self, *args):
        super().on_open()
        self.ws.send('{"sub": "market.btcusdt.depth.step0","id": "id1"}')

    def on_message(self, object, response):
        super().on_message()
        resp = json.loads(gzip.decompress(response).decode('utf-8'))
        # print(resp)
        try:
            self._response_mapping(tuple(list(resp.keys())), resp)
        except KeyError as unknown_response_type:
            self.LOGGER.info(f'Unknown response: {unknown_response_type}')

    def heartbeat_response_creator(self, response):
        """example heartbeat check: {"ping":1644437537572}"""
        self.ws.send('{{"pong": {}}}'.format(response['ping']))
        self.LOGGER.info("Huobi: heartbeat message sent with timestamp {}".format(response['ping']))


    def push_handler(self, response):
        with self.lock:
                self.orderbook_handler[self.name][0] = response['tick']['asks'][0]
                self.orderbook_handler[self.name][1] = response['tick']['bids'][0]


    @property
    def _check_price_match(self):
        return {'bids': self.orderbook_handler[self.name][1][0], 'asks': self.orderbook_handler[self.name][0][0]}

    def _response_mapping(self, response_keys_tuple, response):
        mapping_dict = {tuple(['ch', 'ts', 'tick']): self.push_handler,
                        tuple(['ping']): self.heartbeat_response_creator,
                        tuple(['id', 'status', 'subbed', 'ts']): self.subscription_confirm}

        return mapping_dict[response_keys_tuple](response)

    def subscription_confirm(self, response):
        self.LOGGER.info(f'Subscription confirmed for {response["subbed"].split(".")[1]}')



# class ObProcessing:
#     def __init__(self, orderbook_handler, lock):
#         self.orderbook_handler = orderbook_handler
#         self.lock = lock
#
#     def run(self):
#         while True:
#             with lock:
#                 print(f"Zonda: {orderbook_handler['Zonda']}  ;  Huobi: {orderbook_handler['Huobi']} ")
#                 time.sleep(0.01)


if __name__ == '__main__':
    lock = threading.Lock()
    orderbook_handler = {'Zonda': np.array([[0., 0.], [0., 0.]]),
                         'Huobi': np.array([[0., 0.], [0., 0.]])}

    h = Huobi('wss://api.huobi.pro/ws', 'huobi', orderbook_handler, lock)
    z = Zonda("wss://api.zonda.exchange/websocket/", 'zonda', orderbook_handler, lock)

    h.start()
    z.start()

    # ObProcessing(orderbook_handler, lock).run()