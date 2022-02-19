import requests
import websocket
import threading
from json import loads, dumps
from datetime import datetime
import gzip
import requests
from datetime import datetime
from itertools import combinations
import time



class Client(threading.Thread):
    def __init__(self, url, exchange):
        super().__init__()
        # create websocket connection
        self.ws = websocket.WebSocketApp(
            url=url,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open
        )

        # exchange name
        self.exchange = exchange

    # keep connection alive
    def run(self):
        while True:
            self.ws.run_forever()

    # convert message to dict, process update
    def on_message(self, message):
        pass

    # catch errors
    def on_error(self, error):
        print(error)

    # run when websocket is closed
    def on_close(self):
        print("### closed ###")

    # run when websocket is initialised
    def on_open(self):
        print(f'Connected to {self.exchange}\n')






class Binance(Client):
    def __init__(self, url, exchange, orderbook, lock):
        super().__init__(url, exchange)

        # local data management
        self.orderbook = orderbook[exchange]
        self.lock = lock
        self.updates = 0
        self.last_update = orderbook

    # convert message to dict, process update
    def on_message(self, message):
        data = loads(message)

        # check for orderbook, if empty retrieve
        if len(self.orderbook) == 0:
            for key, value in self.get_snapshot().items():
                self.orderbook[key] = value

        # get lastUpdateId
        lastUpdateId = self.orderbook['lastUpdateId']

        # drop any updates older than the snapshot
        if self.updates == 0:
            if data['U'] <= lastUpdateId+1 and data['u'] >= lastUpdateId+1:
                self.orderbook['lastUpdateId'] = data['u']
                self.process_updates(data)

        # check if update still in sync with orderbook
        elif data['U'] == lastUpdateId+1:
            self.orderbook['lastUpdateId'] = data['u']
            self.process_updates(data)
        else:
            print('Out of sync, abort')

    # Loop through all bid and ask updates, call manage_orderbook accordingly
    def process_updates(self, data):
        with self.lock:
            for update in data['b']:
                self.manage_orderbook('bids', update)
            for update in data['a']:
                self.manage_orderbook('asks', update)
            self.last_update['last_update'] = datetime.now()

    # Update orderbook, differentiate between remove, update and new
    def manage_orderbook(self, side, update):
        # extract values
        price, qty = update

        # loop through orderbook side
        for x in range(0, len(self.orderbook[side])):
            if price == self.orderbook[side][x][0]:
                # when qty is 0 remove from orderbook, else
                # update values
                if qty == 0:
                    del self.orderbook[side]
                    break
                else:
                    self.orderbook[side][x] = update
                    break
            # if the price level is not in the orderbook,
            # insert price level, filter for qty 0
            elif ((price > self.orderbook[side][x][0] and side == 'bids') or
                    (price < self.orderbook[side][x][0] and side == 'asks')):
                if qty != 0:
                    self.orderbook[side].insert(x, update)
                    break
                else:
                    break

    # retrieve orderbook snapshot
    def get_snapshot(self):
        r = requests.get('https://www.binance.com/api/v1/depth?symbol=STEEMBTC&limit=1000')
        return loads(r.content.decode())



class Upbit(Client):
    def __init__(self, url, exchange, orderbook, lock):
        super().__init__(url, exchange)

        # local data management
        self.orderbook = orderbook[exchange]
        self.lock = lock
        self.updates = 0
        self.last_update = orderbook

    # convert message to dict, process update
    def on_message(self, message):
        data = loads(message)

        # first message is the full snapshot
        if data['method'] == 'snapshotOrderbook':
            # extract params
            params = data['params']

            # convert data to right format
            self.orderbook['bids'] = [[x['price'], x['size']] for x in params['bid']]
            self.orderbook['asks'] = [[x['price'], x['size']] for x in params['ask']]

            self.orderbook['sequence'] = params['sequence']
        # following messages are updates to the orderbook
        elif data['method'] == 'updateOrderbook':
            # extract params
            params = data['params']

            # track sequence to stay in sync
            if params['sequence'] == self.orderbook['sequence']+1:
                self.orderbook['sequence'] = params['sequence']
                self.process_updates(params)
            else:
                print('Out of sync, abort')

    # Loop through all bid and ask updates, call manage_orderbook accordingly
    def process_updates(self, data):
        with self.lock:
            for update in [[x['price'], x['size']] for x in data['bid']]:
                self.manage_orderbook('bids', update)
            for update in [[x['price'], x['size']] for x in data['ask']]:
                self.manage_orderbook('asks', update)
            self.last_update['last_update'] = datetime.now()

    # Update orderbook, differentiate between remove, update and new
    def manage_orderbook(self, side, update):
        # extract values
        price, qty = update

        # loop through orderbook side
        for x in range(0, len(self.orderbook[side])):
            if price == self.orderbook[side][x][0]:
                # when qty is 0 remove from orderbook, else
                # update values
                if qty == 0:
                    del self.orderbook[side]
                    break
                else:
                    self.orderbook[side][x] = update
                    break
            # if the price level is not in the orderbook,
            # insert price level, filter for qty 0
            elif ((price > self.orderbook[side][x][0] and side == 'bids') or
                    (price < self.orderbook[side][x][0] and side == 'asks')):
                if qty != 0:
                    self.orderbook[side].insert(x, update)
                    break
                else:
                    break

    # register to orderbook stream
    def on_open(self):
        super().on_open()
        params = {
            "method": "subscribeOrderbook",
            "params": {
                "symbol": "STEEMBTC"
            },
            "id": 1
        }
        self.ws.send(dumps(params))


# inherits from Client
class Huobi(Client):
    # call init from parent class
    def __init__(self, url, exchange, orderbook, lock):
        super().__init__(url, exchange)

        # local data management
        self.orderbook = orderbook[exchange]
        self.lock = lock
        self.last_update = orderbook

    # convert message to dict, decode, extract top ask/bid
    def on_message(self, message):
        data = loads(gzip.decompress(message).decode('utf-8'))

        # extract bids/aks
        if 'tick' in data:
            with self.lock:
                self.orderbook['bids'] = data['tick']['bids']
                self.orderbook['asks'] = data['tick']['asks']
                self.last_update['last_update'] = datetime.now()

        # respond to ping message
        elif 'ping' in data:
            params = {"pong": "data['ping']"}
            self.ws.send(dumps(params))

    # convert dict to string, subscribe to data streem by sending message
    def on_open(self):
        super().on_open()
        params = {"sub": "market.steembtc.depth.step0", "id": "id1"}
        self.ws.send(dumps(params))


# -*- coding: utf-8 -*-




# calculate absolute delta in percentage
def delta(v1, v2):
    return abs((v2-v1)/v1)*100


# retrieve top bid/ask for each exchange
# calculate deltas
def calculate_price_delta(orderbooks, ex1, ex2):
    bid1 = float(orderbooks[ex1]['bids'][0][0])
    bid2 = float(orderbooks[ex2]['bids'][0][0])

    ask1 = float(orderbooks[ex1]['asks'][0][0])
    ask2 = float(orderbooks[ex2]['asks'][0][0])

    bid = delta(bid1, bid2)
    ask = delta(ask1, ask2)

    print(f'{ex1}-{ex2}\tBID Δ: {bid:.2f}% ASK Δ: {ask:.2f}%')

# return subsets of size 2 of all exchanges
def exchange_sets(orderbooks):
    exchanges = []

    # extract exchanges
    for exchange in orderbooks:
        if exchange != 'last_update':
            exchanges.append(exchange)

    # return all subsets
    return list(combinations(exchanges, 2))

# print top bid/ask for each exchange
# run forever
def run(orderbooks, lock):
    # local last_update
    current_time = datetime.now()

    # exchange subsets
    sets = exchange_sets(orderbooks)

    while True:
        try:
            # check for new update
            if orderbooks['last_update'] != current_time:
                with lock:
                    # extract and print data
                    for exchanges in sets:
                        ex1, ex2 = exchanges
                        calculate_price_delta(orderbooks, ex1, ex2)
                    print(f"Last update: {orderbooks['last_update']}\n")

                    # set local last_update to last_update
                    current_time = orderbooks['last_update']
            time.sleep(0.1)
        except Exception:
            pass


if __name__ == "__main__":
    # data management
    lock = threading.Lock()
    orderbooks = {
        "Binance": {},
        "Huobi": {},
        "Upbit": {},
        "last_update": None,
    }

    # create websocket threads
    binance = Binance(
        url="wss://stream.binance.com:9443/ws/steembtc@depth",
        exchange="Binance",
        orderbook=orderbooks,
        lock=lock,
    )

    huobi = Huobi(
        url="wss://api.huobipro.com/ws",
        exchange="Huobi",
        orderbook=orderbooks,
        lock=lock,
    )

    upbit = Upbit(
        url="wss://api.hitbtc.com/api/2/ws",
        exchange="Upbit",
        orderbook=orderbooks,
        lock=lock,
    )


    # start threads
    binance.start()
    huobi.start()
    upbit.start()

    # process websocket data
    run(orderbooks, lock)