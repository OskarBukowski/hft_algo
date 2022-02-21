import numpy as np

b = {'action': 'push',
     'topic': 'trading/orderbook-limited/btc-pln/10',
     'message': {'changes':
                     [
                         {'marketCode': 'BTC-PLN', 'entryType': 'Buy', 'rate': '152770.57', 'action': 'update', 'state': {'ra': '152770.57', 'ca': '0.06244586', 'sa': '0.06244586', 'pa': '0.06244586', 'co': 2}},
                         {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '153401.03', 'action': 'update', 'state': {'ra': '153401.03', 'ca': '0.32513288', 'sa': '0.32513288', 'pa': '0.32513288', 'co': 1}},
                         {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '153401.04', 'action': 'update', 'state': {'ra': '153401.04', 'ca': '0.6000', 'sa': '0.6000', 'pa': '0.6000', 'co': 1}},
                         {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '153401.05', 'action': 'remove'}, {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '153401.06', 'action': 'remove'}],
         'timestamp': '1645474983307'}, 'timestamp': '1645474983307', 'seqNo': 96407614}


orderbook_handler = {'Zonda': np.array([[0., 0.], [0., 0.]]),
                         'Huobi': np.array([[0., 0.], [0., 0.]])}



for r in b['message']['changes']:
    if r['rate'] == orderbook_handler['Zonda'][0][0]:
        pass
