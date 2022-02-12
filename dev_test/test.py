######
# In this script I have the proxy response with up to 300 lines of orderbook
# I need to create the async loop that will receive this data every few miliseconds
#####


import json
import psycopg2

tr = {'ch': 'market.btcusdt.trade.detail',
      'ts': 1644438174613,
      'tick': {'id': 149028017931,
               'ts': 1644438174611,
               'data': [
                   {'id': 149028017931472898124980116,
                    'ts': 1644438174611,
                    'tradeId': 102627793342,
                    'amount': 0.000330785964198955,
                    'price': 44466.86,
                    'direction': 'buy'},
                   {'id': 149028017931472898098574253,
                    'ts': 1644438174611,
                    'tradeId': 102627793341,
                    'amount': 0.0002,
                    'price': 44463.96,
                    'direction': 'buy'},
                   {'id': 149028017931472898116171596,
                    'ts': 1644438174611,
                    'tradeId': 102627793340,
                    'amount': 0.001367,
                    'price': 44463.62,
                    'direction': 'buy'},
                   {'id': 149028017931472898107454491, 'ts': 1644438174611, 'tradeId': 102627793339, 'amount': 0.000138,
                    'price': 44463.61, 'direction': 'buy'},
                   {'id': 149028017931472898124590561, 'ts': 1644438174611, 'tradeId': 102627793338, 'amount': 0.002608,
                    'price': 44463.61, 'direction': 'buy'},
                   {'id': 149028017931472898116205869, 'ts': 1644438174611, 'tradeId': 102627793337,
                    'amount': 6.706930971192e-05, 'price': 44463.61, 'direction': 'buy'}]}}


# if 'ch' in tr:
#     print('yesy')
# else:
#     print('no')
#
# # print(tr['tick']['data']) # list
#
print(tr.keys())
#
# print(tr['tick']['data'][0]['tradeId'])  # id
#
# print(tr['tick']['data'][0]['price'])  # price
#
# print(tr['tick']['data'][0]['amount'])  # amount
#
# print(tr['tick']['data'][0]['ts']) # timestamp
#
# print(tr['ch'].split(".")[1])

