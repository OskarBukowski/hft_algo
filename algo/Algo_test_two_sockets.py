import websockets
import asyncio
import json
import threading
import concurrent.futures


async def zonda():
    async with websockets.connect("wss://api.zonda.exchange/websocket/") as ws:
        message_send = '{"action": "subscribe-public","module": "trading","path": "transactions/btc-pln"}'
        await ws.send(message_send)
        while True:
            await ws.recv()


async def gemini():
    async with websockets.connect("wss://api.gemini.com/v1/marketdata/ftmusd?trades=true") as ws:
        while True:
            await ws.recv()


async def main():
    all_connections = [gemini(), zonda()]
    results = await asyncio.gather(*all_connections)
    print(results)


if __name__ == '__main__':
    asyncio.run(main())



