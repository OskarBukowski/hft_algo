import websockets
import asyncio
import json
import threading


class Websocket():
    # async def zonda(self):
    #     async for websocket in  websockets.connect("wss://api.zonda.exchange/websocket/",
    #                                   ping_timeout=30, close_timeout=20):
    #         message_send = '{"requestId": "78539fe0-e9b0-4e4e-8c86-70b36aa93d4f","action": "proxy","module": "trading","path": "orderbook-limited/btc-pln"}'
    #         await websocket.send(message_send)
    #         while True:
    #             response = await websocket.recv()
    #             message_recv = json.loads(response)
    #             print(message_recv)


    async def gemini(self):
        async for websocket in websockets.connect("wss://api.gemini.com/v2/marketdata",
                                      ping_timeout=30, close_timeout=20):

            await websocket.send('{"type": "subscribe","subscriptions":[{"name":"l2","symbols":["BTCUSD"]}]}')
            while True:
                resp = await websocket.recv()
                response = json.loads(resp)
                print(response)


async def main():
    socket_class = Websocket()
    all_connections = [socket_class.gemini()]
    await asyncio.gather(*all_connections)

if __name__ == '__main__':
    asyncio.run(main())