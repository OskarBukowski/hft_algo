#!/usr/bin/env python3

import sys
sys.path.append("/home/obukowski/Desktop/hft_algo")
sys.path.append("/home/obukowski/Desktop/repo/hft_algo")

import websocket
import threading
import numpy as np
import json
import gzip
import time
from operator import itemgetter
from admin.admin_tools import logger_conf


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
        self.LOGGER.error(repr(args), exc_info=True)

    def on_close(self, *args):
        self.LOGGER.info(f"### Received closing message for {self.exchange}###")

    def on_open(self, *args):
        """There we need only one argument, because the whole response is <websocket._app.WebSocketApp object>"""
        self.LOGGER.info(f'Subscribed for {self.exchange} websocket')


class Zonda(Client):
    LOGGER = logger_conf("../algo/two_socket_test.log")
    PUSH_COUNTER = 0

    def __init__(self, url, exchange, orderbook_handler, lock):
        super().__init__(url, exchange)

        self.url = url
        self.exchange = exchange
        self.orderbook_handler = orderbook_handler
        self.lock = lock
        self.internal_ob = self.model_ob_creator()
        self.seq_no = None

    def heartbeat(func):
        """Decorator that send heartbeat message every 20 push received"""

        def heartbeat_sender(self, *args):
            func(self, *args)
            if self.PUSH_COUNTER == 20:
                self.LOGGER.info("Zonda: Sending heartbeat request")
                self.ws.send('{"action": "ping"}')
                self.PUSH_COUNTER = 0

        return heartbeat_sender

    def seqNo_follower(func):
        """Decorator that checks if the seqNo in push message haven't been lost"""

        def handler(self, *args):
            previous_seqNo = self.seq_no
            func(self, *args)
            if int(self.seq_no) != int(previous_seqNo) + 1:
                self.LOGGER.warning("Zonda: !!! Missed push sequence !!!")
                self.LOGGER.warning(f"Zonda: Current sequence number {self.seq_no}, previous: {previous_seqNo}")

        return handler

    @staticmethod
    def model_ob_creator():
        return {'ask': {i: [0.0, 0.0] for i in range(10)},
                'bid': {i: [0.0, 0.0] for i in range(10)}}

    @property
    def name(self):
        return type(self).__name__

    def heartbeat_confirm(self, response):
        self.LOGGER.info(
            f'Zonda: heartbeat message confirmed with {response["action"]} ; received on {round(int(time.time()) * 1000)}')

    def on_open(self, *args):
        super().on_open()
        self._snapshot()
        self.LOGGER.info("Zonda: Sending subscription request")
        self.ws.send('{"action": "subscribe-public","module": "trading","path": "orderbook-limited/btc-pln/10"}')

    def _snapshot(self):
        self.LOGGER.info("Sending snapshot request")
        return self.ws.send(
            '{"requestId": "78539fe0-e9b0-4e4e-8c86-70b36aa93d4f","action": "proxy","module": "trading",'
            '"path": "orderbook-limited/btc-pln/10"}')

    def on_message(self, object, response):
        response = json.loads(response)
        # print(response)
        try:
            self.LOGGER.info("Zonda: Push response received, starting mapping")
            self._response_mapping(tuple(list(response.keys())), response)
        except KeyError:
            self.LOGGER.error(f'Zonda: Unable to handle response: {response}', exc_info=True)

    def snapshot_handler(self, response):
        self.LOGGER.info(f'Zonda: Activating snapshot handler')
        with self.lock:
            self.seq_no = response['body']['seqNo']
            self.internal_ob['ask'] = {i: [[float(e['ra']), float(e['ca'])] for e in response['body']['sell']][i] for i
                                       in range(10)}
            self.internal_ob['bid'] = {i: sorted([[float(e['ra']), float(e['ca'])] for e in response['body']['buy']],
                                                 key=itemgetter(0), reverse=True)[i] for i in range(10)}
            self.orderbook_handler[self.name][0] = [float(response['body']['sell'][0][a]) for a in ['ra', 'ca']]
            self.orderbook_handler[self.name][1] = [float(response['body']['buy'][-1][a]) for a in ['ra', 'ca']]

            self.LOGGER.info("Zonda: Snapshot handler confirmed")
            self.LOGGER.info(f'Zonda: Snapshot handler output: {f"{self.name}: {self.orderbook_handler[self.name]}"}')

    def internal_ask(self, push):
        self.asks = [i for i in self.internal_ob['ask'].values()]
        if push['action'] == 'update':
            if min([i[0] for i in self.asks]) <= float(push['rate']) <= max([i[0] for i in self.asks]):
                try:
                    index = [i[0] for i in self.asks].index(float(push['rate']))
                    self.internal_ob['ask'][index][1] = float(push['state']['ca'])
                except ValueError:
                    self.asks.append([float(push['state']['ra']), float(push['state']['ca'])])
                    self.asks = sorted(self.asks, key=itemgetter(0), reverse=False)
                    self.asks.remove(self.asks[-1])
                    self.internal_ob['ask'] = {k: self.asks[k] for k, v in self.internal_ob['ask'].items()}

            elif float(push['rate']) < min([i[0] for i in self.asks]):
                self.asks.append([float(push['state']['ra']), float(push['state']['ca'])])
                self.asks = sorted(self.asks, key=itemgetter(0), reverse=False)
                self.asks.remove(self.asks[-1])
                self.internal_ob['ask'] = {k: self.asks[k] for k, v in self.internal_ob['ask'].items()}

            else:
                self.LOGGER.info('Zonda: Unable to find given rate, check seqNo')

        elif push['action'] == 'remove':
            try:
                index = [i[0] for i in self.asks].index(float(push['rate']))
                self.internal_ob['ask'][index] = [100000000.0, 1000000000.0]
                self.asks = sorted([i for i in self.internal_ob['ask'].values()], key=itemgetter(0))
                self.internal_ob['ask'] = {k: self.asks[k] for k, v in self.internal_ob['ask'].items()}
            except ValueError:
                self.LOGGER.info('Zonda: Unable to find given rate, check seqNo')

    def internal_bid(self, push):
        self.bids = [i for i in self.internal_ob['bid'].values()]
        if push['action'] == 'update':
            if min([i[0] for i in self.bids]) <= float(push['rate']) <= max([i[0] for i in self.bids]):
                try:
                    index = [i[0] for i in self.bids].index(float(push['rate']))
                    self.internal_ob['bid'][index][1] = float(push['state']['ca'])
                except ValueError:
                    self.bids.append([float(push['state']['ra']), float(push['state']['ca'])])
                    self.bids = sorted(self.bids, key=itemgetter(0), reverse=True)
                    self.bids.remove(self.bids[-1])
                    self.internal_ob['bid'] = {k: self.bids[k] for k, v in self.internal_ob['bid'].items()}

            elif float(push['rate']) > max([i[0] for i in self.bids]):
                self.bids.append([float(push['state']['ra']), float(push['state']['ca'])])
                self.bids = sorted(self.bids, key=itemgetter(0), reverse=True)
                self.bids.remove(self.bids[-1])
                self.internal_ob['bid'] = {k: self.bids[k] for k, v in self.internal_ob['bid'].items()}

            else:
                self.LOGGER.info('Zonda: Unable to find given rate, check seqNo')

        elif push['action'] == 'remove':
            try:
                index = [i[0] for i in self.bids].index(float(push['rate']))
                self.internal_ob['bid'][index] = [0.0, 0.0]
                self.bids = sorted([i for i in self.internal_ob['bid'].values()], key=itemgetter(0), reverse=True)
                self.internal_ob['bid'] = {k: self.bids[k] for k, v in self.internal_ob['bid'].items()}
            except ValueError:
                self.LOGGER.info('Zonda: Unable to find given rate, check seqNo')

    @heartbeat
    @seqNo_follower
    def push_handler(self, response):
        self.LOGGER.info(f'Zonda: Activating push handler')
        self.seq_no = response['seqNo']
        for push in response['message']['changes']:
            self.LOGGER.debug(f"On start push handler asks: {self.internal_ob['ask']}")
            self.LOGGER.debug(f"On start push handler bids: {self.internal_ob['bid']}")
            self.LOGGER.debug(f"Action: {push['action']}, Side: {push['entryType']}, Rate: {push['rate']}")
            if push['entryType'] == 'Sell':
                self.internal_ask(push)
            else:
                self.internal_bid(push)

            self.orderbook_handler[self.name][0] = self.internal_ob['ask'][0]
            self.orderbook_handler[self.name][1] = self.internal_ob['bid'][0]

            self.LOGGER.debug(f'Zonda: Asks after operations: rate: {push["rate"]}, asks: {self.internal_ob["ask"]}')
            self.LOGGER.debug(f'Zonda: Bids after operations: rate: {push["rate"]}, bids: {self.internal_ob["bid"]}')

    def _response_mapping(self, response_keys_tuple, response):
        mapping_dict = {tuple(['action', 'requestId', 'statusCode', 'body']): self.snapshot_handler,
                        tuple(['action', 'topic', 'message', 'timestamp', 'seqNo']): self.push_handler,
                        tuple(['action', 'module', 'path']): self.subscription_confirm,
                        tuple(['action']): self.heartbeat_confirm}

        return mapping_dict[response_keys_tuple](response)

    def subscription_confirm(self, response):
        self.LOGGER.info(f'Zonda: Subscription confirmed for {response["path"].split("/")[1]}')


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
        response = json.loads(gzip.decompress(response).decode('utf-8'))
        # print(response)
        self.LOGGER.info(f"Huobi: Push response received, starting mapping")
        try:
            self._response_mapping(tuple(list(response.keys())), response)
        except KeyError as unknown_response_type:
            self.LOGGER.info(f'Huobi: Unknown response: {unknown_response_type}')


    def on_error(self, *args):
        super().on_error()
        self.LOGGER.warning(f"Huobi: Received error, reconnecting to {self.name}")
        super().on_open()
        self.on_open()

    def heartbeat_response_creator(self, response):
        """example heartbeat check: {"ping":1644437537572}"""
        self.ws.send('{{"pong": {}}}'.format(response['ping']))
        self.LOGGER.info("Huobi: heartbeat message sent with timestamp {}".format(response['ping']))

    def push_handler(self, response):
        self.LOGGER.info(f"Huobi: Activating push handler")
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
        self.LOGGER.info(f'Huobi: Subscription confirmed for {response["subbed"].split(".")[1]}')


class ObProcessing:
    def __init__(self, orderbook_handler, lock):
        self.orderbook_handler = orderbook_handler
        self.lock = lock

    def run(self):
        while True:
            with self.lock:
                print(f"Zonda: {self.orderbook_handler['Zonda']}  ;  Huobi: {self.orderbook_handler['Huobi']} ")



if __name__ == '__main__':

    try:
        lock = threading.Lock()
        orderbook_handler = {'Zonda': np.array([[0., 0.], [0., 0.]]),
                             'Huobi': np.array([[0., 0.], [0., 0.]])}

        h = Huobi('wss://api.huobi.pro/ws', 'huobi', orderbook_handler, lock)
        z = Zonda("wss://api.zonda.exchange/websocket/", 'zonda', orderbook_handler, lock)

        h.start()
        z.start()

        processor = ObProcessing(orderbook_handler, lock)
        processor.run()

    except KeyboardInterrupt as signal_error:
        h.LOGGER.info("Received closing order from user")
        h.LOGGER.info("Closing application")


