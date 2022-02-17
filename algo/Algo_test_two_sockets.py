import requests
import websocket
import threading
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
    def __init__(self, url, exchange, message_to_sent):
        super().__init__()
        self.ws = websocket.WebSocketApp(
            url=url,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open)

        self.exchange = exchange
        self.message = message_to_sent

    def run(self, *args):
        while True:
            self.ws.run_forever()

    def on_message(self, *args):
        print(args[1])

    def on_error(self, *args):
        print(args[1])

    def on_close(self, *args):
        print("### closed ###", args)

    def on_open(self, *args):
        """There we need only one argument, because the whole response is <websocket._app.WebSocketApp object>"""
        print(f'Connected to Exchange', self.exchange)
        self.ws.send(self.message)






if __name__ == '__main__':
    c = Client('wss://api.huobi.pro/ws', 'huobi', '{"sub": "market.btcusdt.depth.step0","id": "id1"}')
    b = Client("wss://api.zonda.exchange/websocket/", 'zonda', '{"action": "subscribe-public","module": "trading",'
                                                               '"path": "orderbook-limited/btc-pln/10"}')
    c.start()
    b.start()
