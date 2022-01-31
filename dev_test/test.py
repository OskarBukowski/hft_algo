######
# In this script I have the proxy response with up to 300 lines of orderbook
# I need to create the async loop that will receive this data every few miliseconds
#####


import json

# exchange_spec_dict = json.load(open("../admin/exchanges"))
#
# print(exchange_spec_dict)


#
#
# data = json.load(open('../admin/exchanges'))
#
# print(data['currency_mapping']['symbols']['btcpln'])
#
# print(data['source']['rest_url'])

#
# class Periodic:
#     def __init__(self, func, time):
#         self.func = func
#         self.time = time
#         self.is_started = False
#         self._task = None
#
#     async def start(self):
#         if not self.is_started:
#             self.is_started = True
#             # Start task to call func periodically:
#             self._task = asyncio.ensure_future(self._run())
#
#     async def stop(self):
#         if self.is_started:
#             self.is_started = False
#             # Stop task and await it stopped:
#             self._task.cancel()
#             with suppress(asyncio.CancelledError):
#                 await self._task
#
#     async def _run(self):
#         while True:
#             await asyncio.sleep(self.time)
#             self.func()
#
# async def main():
#     p = Periodic(lambda: print('test'), 1)
#     try:
#         print('Start')
#         await p.start()
#         await asyncio.sleep(3.1)
#
#         print('Stop')
#         await p.stop()
#         await asyncio.sleep(3.1)
#
#     finally:
#         await p.stop()  # we should stop task finally
#
#
# if __name__ == '__main__':
#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(main())



waz = {'data':
    {'trades': [
        {'E': 1643657857000,
         'S': 'buy',
         'a': 2221415665,
         'b': 2221415716,
         'm': True,
         'p': '3057165.0',
         'q': '0.01799',
         's': 'btcinr',
         't': 336857947}]},
    'stream': 'btcinr@trades'}

waz_2 = {'data': {'trades': [
    {'E': 1643657858000, 'S': 'buy', 'a': 2221406473, 'b': 2221415856, 'm': True, 'p': '213039.5', 'q': '0.0234',
     's': 'ethinr', 't': 336857959}]}, 'stream': 'ethinr@trades'}

conn = {'data': {'timeout_duration': 1800}, 'event': 'connected'}
conn_2 = {'data': {'streams': ['btcinr@trades', 'ethinr@trades']}, 'event': 'subscribed', 'id': 0}


# print(waz['data']['trades'][0]['p']) # price
# print(waz['data']['trades'][0]['q']) # volume
# print(waz['data']['trades'][0]['a']) # id [ we take the buyer id as a main id ]
# print(waz['data']['trades'][0]['E']) # timestamp

print(waz['data']['trades'][0]['s']) # price

r = {"data":{"code":400,"message":"Invalid request: ID must be an unsigned integer","id":0}}
print(r['data']['message'])
print(list(waz.keys()))
print(conn_2['data']['message']) # equal to the part of url
