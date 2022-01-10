import websockets
import asyncio



async def main():
    async with websockets.connect("wss://api.zonda.exchange/websocket/") as websocket:
        message = '{"action": "subscribe-public","module": "trading","path": "orderbook/btc-pln"}'
        await websocket.send(message)
        while True:
            try:
                message = await websocket.recv()
                print(message)

            except Exception as e: # there is the place for alert sended via messenger/slack/discord etc.
                print(e)



asyncio.get_event_loop().run_until_complete(main())
asyncio.get_event_loop().run_forever()  # it will skip the error and save ob without brakes




