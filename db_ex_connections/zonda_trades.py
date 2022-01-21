#####
# This script will use push in websocket APi to get the update of last trades
# logs of this script will be saved into the same file like orderbooks to keep
# to clarity of data, and reduce the infrastructure complexity

### SCRIPT WORKS --> SAVES TO POSTGRES --> SAVES TO LOGFILE


import websockets
import asyncio
from admin_tools.admin_tools import connection, logger_conf


async def main():
    cursor = connection()
    async with websockets.connect("wss://api.zonda.exchange/websocket/",
                                  ping_timeout=30,
                                  close_timeout=20) as websocket:

        message_send = '{"action": "subscribe-public","module": "trading","path": "transactions/eth-pln"}'
        await websocket.send(message_send)
        while True:
            try:
                message_recv = await websocket.recv().json()
                cursor.execute(
                    f'''INSERT INTO zonda.zonda_socket_trades (id, price, volume, "timestamp")
                                                            VALUES (
                                                                '{str(message_recv['message']['transactions'][0]['id'])}',
                                                                '{str(message_recv['message']['transactions'][0]['r'])}',
                                                                {float(message_recv['message']['transactions'][0]['a'])},
                                                                {int(message_recv['timestamp'])}
                                                        )'''
                )
                print(message_recv)
                logger_conf().info(f"Trades received on timestamp: {message_recv['timestamp']}")

            except Exception as websocket_error:
                logger_conf().error(f" $$ {websocket_error} $$ ", exc_info=True)



asyncio.get_event_loop().run_until_complete(main())
asyncio.get_event_loop().run_forever()
