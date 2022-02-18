import requests
import websocket
import threading
import numpy as np
import json
from datetime import datetime
import gzip
import requests
from datetime import datetime
from itertools import combinations
import time


# TO DO:
# 1. Create classes for two instruments with inheritance from Client class
# 2. Modify Client() class to have stable response for given events, deal with WebsocketApp() class response
# 3. Check the lock() option to get synchonized responses


class Client(threading.Thread):
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
        print(args[1])

    def on_close(self, *args):
        print("### closed ###", args)

    def on_open(self, *args):
        """There we need only one argument, because the whole response is <websocket._app.WebSocketApp object>"""
        print(f'Connected to Exchange', self.exchange)


class Zonda(Client):
    def __init__(self, url, exchange, orderbook_handler):
        super().__init__(url, exchange)

        self.url = url
        self.exchange = exchange
        self.orderbook_handler = orderbook_handler

    @property
    def name(self):
        return type(self).__name__

    def on_open(self, *args):
        super().on_open()
        self.snapshot()
        self.ws.send('{"action": "subscribe-public","module": "trading","path": "orderbook-limited/btc-pln/10"}')


    def snapshot(self):
        return self.ws.send(
            '{"requestId": "78539fe0-e9b0-4e4e-8c86-70b36aa93d4f","action": "proxy","module": "trading",'
            '"path": "orderbook-limited/btc-pln/10"}')

    def on_message(self, object, response):
        response = json.loads(response)
        print(response)

        if [response[i] for i in ['action', 'statusCode']] == ['proxy-response', 200]:
            ask = [float(response['body']['sell'][0][i]) for i in ['ra', 'ca']]  # first line ask
            bid = [float(response['body']['buy'][-1][i]) for i in ['ra', 'ca']]  # first line bid

            print(bid)
            print(ask)

    def snapshot_handler(self):
        pass

    def push_handler(self, response):
        pass

    def _response_mapping(self, response_keys_tuple, response):
        """dict_keys(['action', 'requestId', 'statusCode', 'body'])  --> snapshot
           dict_keys(['action', 'topic', 'message', 'timestamp', 'seqNo'])  --> push"""

        mapping_dict = {tuple(['action', 'requestId', 'statusCode', 'body']): self.snapshot_handler,
                        tuple(['action', 'topic', 'message', 'timestamp', 'seqNo']): self.push_handler}

        return mapping_dict[response_keys_tuple](response)




class Huobi(Client):
    def __init__(self, url, exchange):
        super().__init__(url, exchange)

        self.url = url
        self.exchange = exchange

    @property
    def name(self):
        return type(self).__name__


    def on_open(self, *args):
        super().on_open()
        self.ws.send('{"sub": "market.btcusdt.depth.step0","id": "id1"}')

    def on_message(self, object, response):
        super().on_message()
        print((gzip.decompress(response).decode('utf-8')))


if __name__ == '__main__':
    orderbook_handler = {'Zonda': np.array([[0, 0], [0, 0]]),
                         'Huobi': np.array([[0, 0], [0, 0]])}

    h = Huobi('wss://api.huobi.pro/ws', 'huobi')
    z = Zonda("wss://api.zonda.exchange/websocket/", 'zonda', orderbook_handler)

    h.start()
    z.start()
