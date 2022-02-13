import websockets
import asyncio
from admin.admin_tools import logger_conf
import json



async def main():
    logger = logger_conf("../algo/zonda_ob_socket_test.log")
    async with websockets.connect("wss://api.zonda.exchange/websocket/",
                                  ping_timeout=30,
                                  close_timeout=20) as websocket:

        message_send = '{"action": "subscribe-public","module": "trading","path": "orderbook/btc-pln"}'
        await websocket.send(message_send)
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
