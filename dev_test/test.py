# import numpy as np
# from operator import itemgetter
#
# s = {'action': 'proxy-response',
#      'requestId': '78539fe0-e9b0-4e4e-8c86-70b36aa93d4f',
#      'statusCode': 200,
#      'body': {
#          'status': 'Ok',
#          'sell': [
#              {'ra': '180955.56', 'ca': '0.00006', 'sa': '0.00006', 'pa': '0.00006', 'co': 1},
#              {'ra': '180955.57', 'ca': '0.02208724', 'sa': '0.02208724', 'pa': '0.02208724', 'co': 1},
#              {'ra': '180955.58', 'ca': '0.02616093', 'sa': '0.03218148', 'pa': '0.02616093', 'co': 1},
#              {'ra': '180999.63', 'ca': '0.09169371', 'sa': '0.09169371', 'pa': '0.09169371', 'co': 1},
#              {'ra': '181000.01', 'ca': '0.69950000', 'sa': '0.69950000', 'pa': '0.69950000', 'co': 1},
#              {'ra': '181953.20', 'ca': '0.44740879', 'sa': '0.44740879', 'pa': '0.44740879', 'co': 1},
#              {'ra': '181954.21', 'ca': '1.012', 'sa': '1.012', 'pa': '1.012', 'co': 1},
#              {'ra': '181956.25', 'ca': '0.01958564', 'sa': '0.01958564', 'pa': '0.01958564', 'co': 1},
#              {'ra': '181956.26', 'ca': '10.297', 'sa': '10.297', 'pa': '10.297', 'co': 1}, {
#                  'ra': '182741.69', 'ca': '1.5000', 'sa': '1.5000', 'pa': '1.5000', 'co': 1}],
#          'buy': [
#              {'ra': '180000', 'ca': '0.01173055', 'sa': '0.01173055', 'pa': '0.01173055', 'co': 3},
#              {'ra': '180001', 'ca': '0.00123888', 'sa': '0.00123888', 'pa': '0.00123888', 'co': 1},
#              {'ra': '180001.12', 'ca': '0.00277776', 'sa': '0.00277776', 'pa': '0.00277776', 'co': 1},
#              {'ra': '180003.58', 'ca': '0.00468752', 'sa': '0.00468752', 'pa': '0.00468752', 'co': 1},
#              {'ra': '180100', 'ca': '0.45795317', 'sa': '0.5', 'pa': '0.45795317', 'co': 1},
#              {'ra': '180100.01', 'ca': '0.05820511', 'sa': '0.05820511', 'pa': '0.05820511', 'co': 1},
#              {'ra': '180109.07', 'ca': '0.00006', 'sa': '0.00006', 'pa': '0.00006', 'co': 1},
#              {'ra': '180200', 'ca': '0.00027747', 'sa': '0.00027747', 'pa': '0.00027747', 'co': 1},
#              {'ra': '180205.97', 'ca': '1.012', 'sa': '1.012', 'pa': '1.012', 'co': 1},
#              {'ra': '180205.98', 'ca': '0.01499059', 'sa': '0.01499059', 'pa': '0.01499059', 'co': 1}],
#          'timestamp': '1646414681548',
#          'seqNo': '98007871'}}
#
# internal_ob = {'ask': {i: [0.0, 0.0] for i in range(10)},
#                'bid': {i: [0.0, 0.0] for i in range(10)}}
#
# print(internal_ob)
#
# internal_ob['ask'] = {i: [[e['ra'], e['ca']] for e in s['body']['sell']][i] for i in range(10)}
# internal_ob['bid'] = {i: sorted([[e['ra'], e['ca']] for e in s['body']['buy']], key=itemgetter(0), reverse=True)[i] for
#                       i in range(10)}
#
# print(internal_ob)
#
# bids = [i for i in internal_ob['bid'].values()]
# asks = [i for i in internal_ob['ask'].values()]
#
# p1 = {'action': 'push',
#       'topic': 'trading/orderbook-limited/btc-pln/10',
#       'message': {'changes': [
#           {'marketCode': 'BTC-PLN', 'entryType': 'Buy', 'rate': '180205.99', 'action': 'update',
#            'state': {'ra': '180205.99', 'ca': '1.012', 'sa': '1.012', 'pa': '1.012', 'co': 1}},
#           {'marketCode': 'BTC-PLN', 'entryType': 'Buy', 'rate': '180205.97', 'action': 'remove'},
#           {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '180999.60', 'action': 'update',
#            'state': {'ra': '180999.60', 'ca': '1.0485091', 'sa': '1.0485091', 'pa': '1.0485091', 'co': 1}},
#           {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '181953.19', 'action': 'update',
#            'state': {'ra': '181953.19', 'ca': '0.02208724', 'sa': '0.02208724', 'pa': '0.02208724', 'co': 1}},
#           {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '182741.69', 'action': 'update',
#            'state': {'ra': '182741.69', 'ca': '1.5000', 'sa': '1.5000', 'pa': '1.5000', 'co': 1}},
#           {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '182741.7', 'action': 'update',
#            'state': {'ra': '182741.7', 'ca': '0.74713181', 'sa': '0.74713181', 'pa': '0.74713181', 'co': 1}},
#           {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '180955.56', 'action': 'remove'},
#           {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '180955.57', 'action': 'remove'},
#           {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '180999.61', 'action': 'remove'},
#           {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '181000.00', 'action': 'remove'}],
#           'timestamp': '1646414682601'},
#       'timestamp': '1646414682601',
#       'seqNo': 98007873}
#
# for c in p1['message']['changes']:
#     push_side = c['entryType']
#     push_rate = c['state']['ra']
#     push_volume = c['state']['ca']
#     pass
#
#
# def internal_ask(self, push, asks):
#     if push['action'] == 'update':
#         if min([i[0] for i in self.asks]) <= float(push['rate']) <= (max([i[0] for i in self.asks]) + 0.01):
#             index = [i[0] for i in self.asks].index(float(push['rate']))
#             self.internal_ob['ask'][index][1] = float(push['state']['ca'])
#
#         elif float(push['rate']) < min([i[0] for i in self.asks]):
#             self.asks.append([float(push['state']['ra']), float(push['state']['ca'])])
#             self.asks = sorted(asks, key=itemgetter(0), reverse=False)
#             self.asks.remove(asks[-1])
#             self.internal_ob['ask'] = {k: self.asks[k] for k, v in self.internal_ob['ask'].items()}
#
#         else:
#             print('Unable to find given rate, check seqNo')
#
#     elif push['action'] == 'remove':
#         try:
#             index = [i[0] for i in self.asks].index(float(push['rate']))
#             self.internal_ob['ask'][index] = [100000000.0, 1000000000.0]
#             self.asks = sorted([i for i in self.internal_ob['ask'].values()], key=itemgetter(0))
#             self.internal_ob['ask'] = {k: self.asks[k] for k, v in self.internal_ob['ask'].items()}
#         except ValueError:
#             print('Unable to find given rate, check seqNo')
#
#
# def internal_bid(self, push, bids):
#     if push['action'] == 'update':
#         if min([i[0] for i in self.bids]) <= float(push['rate']) <= (max([i[0] for i in self.bids]) + 0.01):
#             index = [i[0] for i in self.bids].index(float(push['rate']))
#             self.internal_ob['ask'][index][1] = float(push['state']['ca'])
#
#         elif float(push['rate']) > max([i[0] for i in bids]):
#             self.bids.append([float(push['state']['ra']), float(push['state']['ca'])])
#             self.bids = sorted(self.bids, key=itemgetter(0), reverse=False)
#             self.bids.remove(self.bids[-1])
#             self.internal_ob['bid'] = {k: self.bids[k] for k, v in self.internal_ob['bid'].items()}
#
#         else:
#             print('Unable to find given rate, check seqNo')
#
#     elif push['action'] == 'remove':
#         try:
#             index = [i[0] for i in self.bids].index(float(push['rate']))
#             self.internal_ob['bid'][index] = [0.0, 0.0]
#             self.bids = sorted([i for i in self.internal_ob['bid'].values()], key=itemgetter(0))
#             self.internal_ob['bid'] = {k: self.asks[k] for k, v in self.internal_ob['bid'].items()}
#         except ValueError:
#             print('Unable to find given rate, check seqNo')
#
#
#
# # p2 = {'action': 'push', 'topic': 'trading/orderbook-limited/btc-pln/10', 'message': {'changes': [
# #     {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '180955.57', 'action': 'update',
# #      'state': {'ra': '180955.57', 'ca': '0.02214724', 'sa': '0.02214724', 'pa': '0.02214724', 'co': 2}},
# #     {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '180999.58', 'action': 'update',
# #      'state': {'ra': '180999.58', 'ca': '1.0485091', 'sa': '1.0485091', 'pa': '1.0485091', 'co': 1}},
# #     {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '181952.18', 'action': 'update',
# #      'state': {'ra': '181952.18', 'ca': '0.44740879', 'sa': '0.44740879', 'pa': '0.44740879', 'co': 1}},
# #     {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '180999.60', 'action': 'remove'},
# #     {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '181953.20', 'action': 'remove'},
# #     {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '182741.7', 'action': 'remove'}],
# #                                                                                      'timestamp': '1646414683127'},
# #       'timestamp': '1646414683127', 'seqNo': 98007874}
# # p3 = {'action': 'push', 'topic': 'trading/orderbook-limited/btc-pln/10', 'message': {'changes': [
# #     {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '180955.56', 'action': 'update',
# #      'state': {'ra': '180955.56', 'ca': '0.00006', 'sa': '0.00006', 'pa': '0.00006', 'co': 1}},
# #     {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '180999.55', 'action': 'update',
# #      'state': {'ra': '180999.55', 'ca': '0.08963594', 'sa': '0.08963594', 'pa': '0.08963594', 'co': 1}},
# #     {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '180999.56', 'action': 'update',
# #      'state': {'ra': '180999.56', 'ca': '1.0485091', 'sa': '1.0485091', 'pa': '1.0485091', 'co': 1}},
# #     {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '180955.57', 'action': 'update',
# #      'state': {'ra': '180955.57', 'ca': '0.02208724', 'sa': '0.02208724', 'pa': '0.02208724', 'co': 1}},
# #     {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '180999.58', 'action': 'remove'},
# #     {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '181952.18', 'action': 'remove'},
# #     {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '181953.19', 'action': 'remove'}],
# #                                                                                      'timestamp': '1646414683676'},
# #       'timestamp': '1646414683676', 'seqNo': 98007875}
# # p4 = {'action': 'push', 'topic': 'trading/orderbook-limited/btc-pln/10', 'message': {'changes': [
# #     {'marketCode': 'BTC-PLN', 'entryType': 'Buy', 'rate': '180206.00', 'action': 'update',
# #      'state': {'ra': '180206.00', 'ca': '0.01499059', 'sa': '0.01499059', 'pa': '0.01499059', 'co': 1}},
# #     {'marketCode': 'BTC-PLN', 'entryType': 'Buy', 'rate': '180205.98', 'action': 'remove'},
# #     {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '180999.54', 'action': 'update',
# #      'state': {'ra': '180999.54', 'ca': '1.0485091', 'sa': '1.0485091', 'pa': '1.0485091', 'co': 1}},
# #     {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '181953.20', 'action': 'update',
# #      'state': {'ra': '181953.20', 'ca': '0.44740879', 'sa': '0.44740879', 'pa': '0.44740879', 'co': 1}},
# #     {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '180999.55', 'action': 'remove'},
# #     {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '180999.56', 'action': 'remove'}],
# #                                                                                      'timestamp': '1646414684207'},
# #       'timestamp': '1646414684207', 'seqNo': 98007876}

import psycopg2
import pandas as pd


def remote_connection():
    conn = psycopg2.connect(
        host='***',
        database='postgres',
        user='postgres',
        password='***'
    )
    conn.autocommit = True
    cursor = conn.cursor()

    return cursor


df = pd.read_sql_query("SELECT * FROM bitso.aaveusd_ob", psycopg2.connect(
        host='***',
        database='postgres',
        user='postgres',
        password='***'))

print(df)