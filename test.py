######
# In this script I have the proxy response with up to 300 lines of orderbook
# I need to create the async loop that will receive this data every few miliseconds
#####



# print(d.keys())
# print(d['body']['sell'][0])
# print(d['body']['sell'][1])
# print(d['body']['sell'][2])
# print(d['body']['sell'][3])
# print('----------------------------')
# print(d['body']['buy'][0])
# print(d['body']['buy'][1])
# print(d['body']['buy'][2])
# print(d['body']['buy'][3])

import websockets
# import asyncio
#
#
# async def main():
#     async with websockets.connect("wss://api.zonda.exchange/websocket/",
#                                   ping_timeout=30,
#                                   close_timeout=20) as websocket:
#
#         message_send = '{"requestId": "78539fe0-e9b0-4e4e-8c86-70b36aa93d4g",' \
#                        '"action": "proxy", ' \
#                        '"module": "trading",' \
#                        '"path": "orderbook/btc-pln"' \
#                        '}'
#         await websocket.send(message_send)
#
#         while True:
#             try:
#                 rec = await websocket.recv()
#                 print(rec)
#
#             except:
#                 print('error')
#
# asyncio.get_event_loop().run_until_complete(main())
# asyncio.get_event_loop().run_forever()  # it will skip the error and save ob without brakes




import asyncio
from contextlib import suppress


class Periodic:
    def __init__(self, func, time):
        self.func = func
        self.time = time
        self.is_started = False
        self._task = None

    async def start(self):
        if not self.is_started:
            self.is_started = True
            # Start task to call func periodically:
            self._task = asyncio.ensure_future(self._run())

    async def stop(self):
        if self.is_started:
            self.is_started = False
            # Stop task and await it stopped:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task

    async def _run(self):
        while True:
            await asyncio.sleep(self.time)
            self.func()

async def main():
    p = Periodic(lambda: print('test'), 1)
    try:
        print('Start')
        await p.start()
        await asyncio.sleep(3.1)

        print('Stop')
        await p.stop()
        await asyncio.sleep(3.1)

    finally:
        await p.stop()  # we should stop task finally


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())