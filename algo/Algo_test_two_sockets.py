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
from operator import itemgetter


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
        print('Error appeared', args)

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
        self.internal_ob = self.model_ob_creator()
        # self.bids = [i for i in self.internal_ob['bid'].values()]
        # self.asks = [i for i in self.internal_ob['ask'].values()]

    def heartbeat(func):
        """Decorator that send heartbeat message every 20 push received"""

        def heartbeat_sender(self, *args):
            func(self, *args)
            if self.PUSH_COUNTER == 20:
                self.ws.send('{"action": "ping"}')
                self.PUSH_COUNTER = 0

        return heartbeat_sender

    @staticmethod
    def model_ob_creator():
        return {'ask': {0: [0.0, 0.0],
                        1: [0.0, 0.0],
                        2: [0.0, 0.0],
                        3: [0.0, 0.0],
                        4: [0.0, 0.0]},

                'bid': {0: [0.0, 0.0],
                        1: [0.0, 0.0],
                        2: [0.0, 0.0],
                        3: [0.0, 0.0],
                        4: [0.0, 0.0]}}

    @property
    def name(self):
        return type(self).__name__

    def heartbeat_confirm(self, response):
        self.LOGGER.info(
            f'Zonda: heartbeat message confirmed with {response["action"]} ; received on {round(int(time.time()) * 1000)}')

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
        # self.LOGGER.info(f'Unknown response: {response}')
        # try:
        self._response_mapping(tuple(list(response.keys())), response)
        # except KeyError:
        #     self.LOGGER.info(f'Unable to handle response;: {response}')

    def snapshot_handler(self, response):   # WORKS CORRECTLY
        self.LOGGER.info(f'Smapshot handler activated')
        with self.lock:
            for i in range(5):
                self.internal_ob['ask'][i] = [float(response['body']['sell'][i][a]) for a in ['ra', 'ca']]
                self.internal_ob['bid'][i] = [float(response['body']['buy'][-(i+1)][a]) for a in ['ra', 'ca']]



            self.orderbook_handler[self.name][0] = [float(response['body']['sell'][0][a]) for a in ['ra', 'ca']]
            self.orderbook_handler[self.name][1] = [float(response['body']['buy'][-1][a]) for a in ['ra', 'ca']]

            self.LOGGER.info(f'Snapshot handler output: {self.orderbook_handler[self.name]}')

    @heartbeat
    def push_handler(self, response):
        self.LOGGER.info(f'Push handler activated')
        self.bids = [i for i in self.internal_ob['bid'].values()]
        self.asks = [i for i in self.internal_ob['ask'].values()]

        self.LOGGER.info(f"--------Asks: {self.asks}, Bids: {self.bids}")

        for r in response['message']['changes']:
            if r['entryType'] == 'Sell':
                self.LOGGER.info(f'Response type Sell')
                if float(r['rate']) >= min([i[0] for i in self.asks]) and float(r['rate']) <= (max([i[0] for i in self.asks])+0.01):
                    if r['action'] == 'update':
                        try:
                            index = [i[0] for i in self.asks].index(float(r['rate']))
                            self.LOGGER.info("-----", float(r['state']['ca']))
                            self.internal_ob['ask'][index][1] = float(r['state']['ca'])
                        except ValueError:  # if the element does not exist in list
                            self.LOGGER.info("-----", float(r['state']['ca']))
                            self.asks.append([float(r['state']['ra']), float(r['state']['ca'])])
                            self.asks = sorted(self.asks, key=itemgetter(0), reverse=False)
                            self.asks.remove(self.asks[-1])
                            self.internal_ob['ask'] = {k: self.asks[k] for k, v in self.internal_ob['ask'].items()}

                    elif r['action'] == 'remove':
                        try:
                            """ We collect only first 5 lines, so if one of the is removed I do not search for values below,
                            but i set 10 billion as a price and wait for update that will be in top five range to get rid
                            of this temporary placeholder"""

                            index = [i[0] for i in self.asks].index(float(r['rate']))
                            self.internal_ob['ask'][index] = [100000000.0, 1000000000.0]
                            self.asks = sorted([i for i in self.internal_ob['ask'].values()], key=itemgetter(0))
                            self.internal_ob['ask'] = {k: self.asks[k] for k, v in self.internal_ob['ask'].items()}
                        except ValueError as e:
                            continue


                elif float(r['rate']) < min([i[0] for i in self.asks]):
                    self.asks.append([float(r['state']['ra']), float(r['state']['ca'])])
                    self.asks = sorted(self.asks, key=itemgetter(0))
                    self.asks.remove(self.asks[-1])
                    self.LOGGER.info('Asks:', self.asks)
                    self.internal_ob['ask'] = {k: self.asks[k] for k, v in self.internal_ob['ask'].items()}

            elif r['entryType'] == 'Buy':
                self.LOGGER.info(f'Response type Buy')

                if float(r['rate']) >= min([i[0] for i in self.bids]) and float(r['rate']) <= (max([i[0] for i in self.bids])+0.01):
                    if r['action'] == 'update':
                        try:
                            self.LOGGER.info("Executing before exception 1 ")
                            index = [i[0] for i in self.bids].index(float(r['rate']))
                            self.internal_ob['bid'][index][1] = float(r['state']['ca'])
                            self.LOGGER.info("-----", float(r['state']['ca']))
                        except ValueError:  # if the element does not exist in list
                            self.LOGGER.info("Executing after exception 1 ")
                            self.LOGGER.info("-----", float(r['state']['ca']))
                            self.bids.append([float(r['state']['ra']), float(r['state']['ca'])])
                            self.bids = sorted(self.bids, key=itemgetter(0), reverse=True)
                            self.bids.remove(self.bids[-1])
                            self.internal_ob['bid'] = {k: self.bids[k] for k, v in self.internal_ob['bid'].items()}

                    elif r['action'] == 'remove':
                        try:
                            """ We collect only first 5 lines, so if one of the is removed I do not search for values below,
                            but i set 0.0 as a price and wait for update that will be in top five range to get rid
                            of this temporary placeholder"""
                            self.LOGGER.info("Executing before exception 2 ")
                            self.LOGGER.info(r)
                            index = [i[0] for i in self.bids].index(float(r['rate']))
                            self.internal_ob['bid'][index] = [0.0, 0.0]
                            self.bids = sorted([i for i in self.internal_ob['bid'].values()], key=itemgetter(0),
                                               reverse=True)
                            self.internal_ob['bid'] = {k: self.bids[k] for k, v in self.internal_ob['bid'].items()}
                        except ValueError as e:
                            self.LOGGER.info("Executing after exception 2 ")
                            continue


                elif float(r['rate']) > max([i[0] for i in self.bids]):
                    if r['action'] == 'update':
                        self.LOGGER.info("Executing before exception 4 ")
                        self.bids.append([float(r['state']['ra']), float(r['state']['ca'])])
                        self.bids = sorted(self.bids, key=itemgetter(0))
                        self.bids.remove(self.bids[-1])
                        self.LOGGER.info(self.bids)
                        self.internal_ob['bid'] = {k: self.bids[k] for k, v in self.internal_ob['bid'].items()}

                    elif r['action'] == 'remove':
                        try:
                            self.LOGGER.info("Executing before exception 5 ")
                            index = [i[0] for i in self.bids].index(float(r['rate']))
                            self.internal_ob['bid'][index] = [0.0, 0.0]
                            self.bids = sorted([i for i in self.internal_ob['bid'].values()], key=itemgetter(0),
                                               reverse=True)
                            self.internal_ob['bid'] = {k: self.bids[k] for k, v in self.internal_ob['bid'].items()}
                        except ValueError as e:
                            continue

        self.LOGGER.info(f'Bid: {r["rate"]}, bids: {self.internal_ob["bid"]}')
        self.LOGGER.info(f'Ask: {r["rate"]}, asks: {self.internal_ob["ask"]}')

        # self._check_price_match['Sell'] = self.internal_ob['ask'][0]
        # self._check_price_match['Buy'] = self.internal_ob['bid'][0]

        self.orderbook_handler[self.name][0] = self.internal_ob['ask'][0]
        self.orderbook_handler[self.name][1] = self.internal_ob['bid'][0]

    def ask_push_handler(self):
        pass

    def bid_push_handler(self):
        pass

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


class ObProcessing:
    def __init__(self, orderbook_handler, lock):
        self.orderbook_handler = orderbook_handler
        self.lock = lock

    def run(self):
        while True:
            with lock:
                print(f"Zonda: {orderbook_handler['Zonda']}  ;  Huobi: {orderbook_handler['Huobi']} ")
                time.sleep(0.5)


if __name__ == '__main__':
    lock = threading.Lock()
    orderbook_handler = {'Zonda': np.array([[0., 0.], [0., 0.]]),
                         'Huobi': np.array([[0., 0.], [0., 0.]])}

    h = Huobi('wss://api.huobi.pro/ws', 'huobi', orderbook_handler, lock)
    z = Zonda("wss://api.zonda.exchange/websocket/", 'zonda', orderbook_handler, lock)

    h.start()
    z.start()

    ObProcessing(orderbook_handler, lock).run()
