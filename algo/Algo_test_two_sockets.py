import websockets
import asyncio
import json
import threading

class Zonda(threading.Thread):

    def main(self):
        with websockets.connect("wss://api.zonda.exchange/websocket/",
                                      ping_timeout=30, close_timeout=20) as websocket:
            message_send = '{"action": "subscribe-public","module": "trading","path": "orderbook/btc-pln"}'
            websocket.send(message_send)
            while True:
                response = websocket.recv()
                message_recv = json.loads(response)
                print(message_recv)


class Gemini(threading.Thread):
    def main(self):
        with websockets.connect("wss://api.gemini.com/v1/marketdata/btcusd",
                                      ping_timeout=30, close_timeout=20) as websocket:
            while True:
                resp = websocket.recv()
                response = json.loads(resp)
                print(response)




if __name__ == '__main__':
    Zonda().start()
    Gemini().start()

