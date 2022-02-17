import websockets
import asyncio
from admin.admin_tools import logger_conf
import json



async def main():
    logger = logger_conf("../algo/zonda_ob_socket_test.log")
    async with websockets.connect("wss://api.zonda.exchange/websocket/",
                                  ping_timeout=30,
                                  close_timeout=20) as websocket:

        update = '{"action": "subscribe-public","module": "trading","path": "orderbook-limited/btc-pln/10"}'

        snapshot = '{"requestId": "78539fe0-e9b0-4e4e-8c86-70b36aa93d4f","action": "proxy","module": "trading","path":"orderbook-limited/btc-pln/10"}'

        await websocket.send(snapshot)
        await websocket.send(update)
        while True:
            try:
                response = await websocket.recv()
                message_recv = json.loads(response)
                print(message_recv)
                logger.info(
                    f"Ob received for {message_recv['message']['changes'][0]['marketCode']} timestamp: {message_recv['message']['timestamp']}")

            except KeyError as websocket_error:
                logger.error(f" $$ {websocket_error} $$ ", exc_info=True)


asyncio.run(main())
