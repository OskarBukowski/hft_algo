##
# This script can be used as a check if the connection is still active
##

from websocket import create_connection


ws = create_connection("wss://api.zonda.exchange/websocket/")
ws.send('{"action": "ping"}')

while True:
    print(ws.recv())
