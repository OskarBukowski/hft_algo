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

        print(response, f"Execution time: {round((time.time() - st),5)}s")




asyncio.get_event_loop().run_until_complete(main())
asyncio.get_event_loop().run_forever()