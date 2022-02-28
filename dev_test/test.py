import numpy as np
from operator import itemgetter

s = {'action': 'proxy-response',
     'requestId': '78539fe0-e9b0-4e4e-8c86-70b36aa93d4f',
     'statusCode': 200,
     'body': {'status': 'Ok',
              'sell': [{'ra': '151820', 'ca': '0.00067000', 'sa': '0.00067000', 'pa': '0.00067000', 'co': 1},
                       {'ra': '152029.31', 'ca': '0.00006', 'sa': '0.00006', 'pa': '0.00006', 'co': 1},
                       {'ra': '152100', 'ca': '0.07862616', 'sa': '0.11143937', 'pa': '0.07862616', 'co': 1},
                       {'ra': '152440.52', 'ca': '0.5000', 'sa': '0.5000', 'pa': '0.5000', 'co': 1},
                       {'ra': '152440.53', 'ca': '0.31724181', 'sa': '0.31724181', 'pa': '0.31724181', 'co': 1}],
              'buy': [{'ra': '150703.28', 'ca': '0.00706866', 'sa': '0.00706866', 'pa': '0.00706866', 'co': 1},
                      {'ra': '151000', 'ca': '0.00119866', 'sa': '0.00119866', 'pa': '0.00119866', 'co': 3},
                      {'ra': '151000.01', 'ca': '1.024', 'sa': '1.024', 'pa': '1.024', 'co': 1},
                      {'ra': '151011.23', 'ca': '0.004', 'sa': '0.004', 'pa': '0.004', 'co': 1},
                      {'ra': '151012.26', 'ca': '0.09856179', 'sa': '0.09856179', 'pa': '0.09856179', 'co': 1}],
              'timestamp': '1645547229471', 'seqNo': '96535057'}}


ob = {'ask': {0: [0.0, 0.0],
              1: [0.0, 0.0],
              2: [0.0, 0.0],
              3: [0.0, 0.0],
              4: [0.0, 0.0]},

      'bid': {0: [0.0, 0.0],
              1: [0.0, 0.0],
              2: [0.0, 0.0],
              3: [0.0, 0.0],
              4: [0.0, 0.0]}}



# Initial snapshot save


# for i in range(5):
#     print([float(s['body']['buy'][-(i+1)]['ra']), float(s['body']['buy'][-(i+1)]['ca'])])
#     print([float(s['body']['sell'][i]['ra']), float(s['body']['sell'][i]['ca'])])
#
#     print("=================")
#
#     print([float(s['body']['buy'][-(i+1)][z]) for z in ['ra', 'ca']])
#     print([float(s['body']['sell'][i][z]) for z in ['ra', 'ca']])
#
#     print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")



ob_2 = {'ask': {0: [1.5182e+05, 6.7000e-04],
                1: [1.5202931e+05, 6.0000000e-05],
                2: [1.521000e+05, 7.862616e-02],
                3: [1.5244052e+05, 5.0000000e-01],
                4: [1.5244053e+05, 3.1724181e-01]},

        'bid': {0: [1.5101226e+05, 9.8561790e-02],
                1: [1.5101123e+05, 4.0000000e-03],
                2: [1.5100001e+05, 1.0240000e+00],
                3: [1.51000e+05, 1.19866e-03],
                4: [1.5070328e+05, 7.0686600e-03]}}



bids = [i for i in ob_2['bid'].values()]
asks = [i for i in ob_2['ask'].values()]

bids_prices = [i[0] for i in bids]
asks_prices = [i[0] for i in asks]

# print(f"Bids: {bids}")
# print(f"Asks: {asks}")
#
# print(f"Bids prices: {bids}")
# print(f"Asks prices: {asks}")
#
# print(ob_2['ask'].values())

# Push examples

p1 = {'action': 'push', 'topic': 'trading/orderbook-limited/btc-pln/10', 'message': {'changes': [{'marketCode': 'BTC-PLN', 'entryType': 'Buy', 'rate': '151039.89', 'action': 'update', 'state': {'ra': '151039.89', 'ca': '0.32488036', 'sa': '0.32488036', 'pa': '0.32488036', 'co': 1}},
                                                                                                 {'marketCode': 'BTC-PLN', 'entryType': 'Buy', 'rate': '151012.28', 'action': 'update', 'state': {'ra': '151012.28', 'ca': '0.09245289', 'sa': '0.09245289', 'pa': '0.09245289', 'co': 1}},
                                                                                                 {'marketCode': 'BTC-PLN', 'entryType': 'Buy', 'rate': '151012.27', 'action': 'remove'},
                                                                                                 {'marketCode': 'BTC-PLN', 'entryType': 'Buy', 'rate': '151012.26', 'action': 'remove'},
                                                                                                 {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '152440.51', 'action': 'update', 'state': {'ra': '152440.51', 'ca': '0.34848427', 'sa': '0.34848427', 'pa': '0.34848427', 'co': 2}},
                                                                                                 {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '152440.53', 'action': 'remove'}], 'timestamp': '1645547230006'}, 'timestamp': '1645547230006', 'seqNo': 96535058}



p2 = {'action': 'push', 'topic': 'trading/orderbook-limited/btc-pln/10', 'message': {'changes': [{'marketCode': 'BTC-PLN', 'entryType': 'Buy', 'rate': '151040.90', 'action': 'update', 'state': {'ra': '151040.90', 'ca': '0.08261755', 'sa': '0.08261755', 'pa': '0.08261755', 'co': 1}},
                                                                                                 {'marketCode': 'BTC-PLN', 'entryType': 'Buy', 'rate': '151039.90', 'action': 'update', 'state': {'ra': '151039.90', 'ca': '0.00852979', 'sa': '0.00852979', 'pa': '0.00852979', 'co': 1}},
                                                                                                 {'marketCode': 'BTC-PLN', 'entryType': 'Buy', 'rate': '150703.27', 'action': 'update', 'state': {'ra': '150703.27', 'ca': '0.90000000', 'sa': '0.90000000', 'pa': '0.90000000', 'co': 1}},
                                                                                                 {'marketCode': 'BTC-PLN', 'entryType': 'Buy', 'rate': '151039.88', 'action': 'remove'},
                                                                                                 {'marketCode': 'BTC-PLN', 'entryType': 'Buy', 'rate': '151038.87', 'action': 'remove'},
                                                                                                 {'marketCode': 'BTC-PLN', 'entryType': 'Buy', 'rate': '151012.28', 'action': 'remove'},
                                                                                                 {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '152440.5', 'action': 'update', 'state': {'ra': '152440.5', 'ca': '0.5000', 'sa': '0.5000', 'pa': '0.5000', 'co': 1}},
                                                                                                 {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '152550', 'action': 'update', 'state': {'ra': '152550', 'ca': '0.11896136', 'sa': '0.11896136', 'pa': '0.11896136', 'co': 1}},
                                                                                                 {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '152440.51', 'action': 'update', 'state': {'ra': '152440.51', 'ca': '0.03124246', 'sa': '0.03124246', 'pa': '0.03124246', 'co': 1}},
                                                                                                 {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '152440.52', 'action': 'remove'},
                                                                                                 {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '152440.59', 'action': 'remove'}], 'timestamp': '1645547230536'}, 'timestamp': '1645547230536', 'seqNo': 96535059}



p3 = {'action': 'push', 'topic': 'trading/orderbook-limited/btc-pln/10', 'message': {'changes': [{'marketCode': 'BTC-PLN', 'entryType': 'Buy', 'rate': '151040.91', 'action': 'update', 'state': {'ra': '151040.91', 'ca': '0.40749791', 'sa': '0.40749791', 'pa': '0.40749791', 'co': 2}},
                                                                                                 {'marketCode': 'BTC-PLN', 'entryType': 'Buy', 'rate': '151000.02', 'action': 'update', 'state': {'ra': '151000.02', 'ca': '0.09857731', 'sa': '0.09857731', 'pa': '0.09857731', 'co': 1}},
                                                                                                 {'marketCode': 'BTC-PLN', 'entryType': 'Buy', 'rate': '151040.90', 'action': 'remove'},
                                                                                                 {'marketCode': 'BTC-PLN', 'entryType': 'Buy', 'rate': '151039.89', 'action': 'remove'},
                                                                                                 {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '152440.49', 'action': 'update', 'state': {'ra': '152440.49', 'ca': '0.31724181', 'sa': '0.31724181', 'pa': '0.31724181', 'co': 1}},
                                                                                                 {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '152872.81', 'action': 'update', 'state': {'ra': '152872.81', 'ca': '10.135', 'sa': '10.135', 'pa': '10.135', 'co': 1}},
                                                                                                 {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '152440.5', 'action': 'remove'},
                                                                                                 {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '152440.51', 'action': 'remove'}], 'timestamp': '1645547231077'}, 'timestamp': '1645547231077', 'seqNo': 96535060}


p4 = {'action': 'push', 'topic': 'trading/orderbook-limited/btc-pln/10', 'message': {'changes': [{'marketCode': 'BTC-PLN', 'entryType': 'Buy', 'rate': '150700.00', 'action': 'update', 'state': {'ra': '150700.00', 'ca': '1.00000000', 'sa': '1.00000000', 'pa': '1.00000000', 'co': 1}},
                                                                                                 {'marketCode': 'BTC-PLN', 'entryType': 'Buy', 'rate': '151040.91', 'action': 'update', 'state': {'ra': '151040.91', 'ca': '0.08261755', 'sa': '0.08261755', 'pa': '0.08261755', 'co': 1}},
                                                                                                 {'marketCode': 'BTC-PLN', 'entryType': 'Buy', 'rate': '151000.02', 'action': 'remove'},
                                                                                                 {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '152440.48', 'action': 'update', 'state': {'ra': '152440.48', 'ca': '0.5000', 'sa': '0.5000', 'pa': '0.5000', 'co': 1}},
                                                                                                 {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '152440.49', 'action': 'update', 'state': {'ra': '152440.49', 'ca': '0.03124246', 'sa': '0.03124246', 'pa': '0.03124246', 'co': 1}},
                                                                                                 {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '152872.81', 'action': 'remove'}], 'timestamp': '1645547231607'}, 'timestamp': '1645547231607', 'seqNo': 96535061}

r = {'action': 'push',
      'topic': 'trading/orderbook-limited/btc-pln/10',
      'message': {'changes': [{'marketCode': 'BTC-PLN', 'entryType': 'Buy', 'rate': '151040.92', 'action': 'update', 'state': {'ra': '151040.92', 'ca': '0.42029272', 'sa': '0.42029272', 'pa': '0.42029272', 'co': 3}},
                                                                                                 {'marketCode': 'BTC-PLN', 'entryType': 'Buy', 'rate': '151039.90', 'action': 'remove'},
                                                                                                 {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '152440.46', 'action': 'update', 'state': {'ra': '152440.46', 'ca': '0.5000', 'sa': '0.5000', 'pa': '0.5000', 'co': 1}},
                                                                                                 {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '152440.47', 'action': 'update', 'state': {'ra': '152440.47', 'ca': '0.31444068', 'sa': '0.31444068', 'pa': '0.31444068', 'co': 1}},
                                                                                                 {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '152440.48', 'action': 'remove'},
                                                                                                 {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '152550', 'action': 'remove'}], 'timestamp': '1645547232131'}, 'timestamp': '1645547232131', 'seqNo': 96535062}





for r in r['message']['changes']:
    if r['action'] == 'update':
        print(float(r['state']['ra']))





















