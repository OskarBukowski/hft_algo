#####
# This script can be used as a check if the connection is possible [ e.g. in case of some maintenance ]
#####

import websockets
import asyncio
import time


async def main():
    async with websockets.connect("wss://api.zonda.exchange/websocket/") as ws:
        st = time.time()
        await ws.send('{"action": "ping"}')
        response = await ws.recv()

        if response == '{"action":"pong"}':
            print('Heartbeat available')
        print(response, f"Execution time: {round((time.time() - st),5)}s")




asyncio.run(main())