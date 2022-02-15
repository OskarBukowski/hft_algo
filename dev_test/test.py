######
# In this script I have the proxy response with up to 300 lines of orderbook
# I need to create the async loop that will receive this data every few miliseconds
#####


import json

import pandas as pd
import psycopg2

# --- zonda websocket ob structure

snap = {'action': 'proxy-response',
     'requestId': '78539fe0-e9b0-4e4e-8c86-70b36aa93d4f',
     'statusCode': 200,
     'body': {'status': 'Ok',
              'sell': [{'ra': '175728.69', 'ca': '1.015', 'sa': '1.015', 'pa': '1.015', 'co': 1},
                       {'ra': '175728.7', 'ca': '0.55206437', 'sa': '0.55206437', 'pa': '0.55206437', 'co': 1},
                       {'ra': '175730', 'ca': '0.02162445', 'sa': '0.02162445', 'pa': '0.02162445', 'co': 1},
                       {'ra': '175750', 'ca': '0.02735585', 'sa': '0.02735585', 'pa': '0.02735585', 'co': 1},
                       {'ra': '175999.97', 'ca': '0.08315793', 'sa': '0.08315793', 'pa': '0.08315793', 'co': 1},
                       {'ra': '175999.98', 'ca': '0.00011593', 'sa': '0.00011593', 'pa': '0.00011593', 'co': 1},
                       {'ra': '175999.99', 'ca': '0.5000', 'sa': '0.5000', 'pa': '0.5000', 'co': 1},
                       {'ra': '176000', 'ca': '4.00093904', 'sa': '4.00093904', 'pa': '4.00093904', 'co': 4},
                       {'ra': '176223.12', 'ca': '0.0004', 'sa': '0.0004', 'pa': '0.0004', 'co': 1},
                       {'ra': '176377.50', 'ca': '10.236', 'sa': '10.236', 'pa': '10.236', 'co': 1}],
              'buy': [{'ra': '173926.00', 'ca': '9.970', 'sa': '9.970', 'pa': '9.970', 'co': 1},
                      {'ra': '173980.33', 'ca': '2.0', 'sa': '2.0', 'pa': '2.0', 'co': 1},
                      {'ra': '173980.34', 'ca': '0.6000', 'sa': '0.6000', 'pa': '0.6000', 'co': 1},
                      {'ra': '174334', 'ca': '0.00166984', 'sa': '0.00166984', 'pa': '0.00166984', 'co': 1},
                      {'ra': '174354.60', 'ca': '1.015', 'sa': '1.015', 'pa': '1.015', 'co': 1},
                      {'ra': '174354.83', 'ca': '0.74600386', 'sa': '0.74600386', 'pa': '0.74600386', 'co': 1},
                      {'ra': '174354.84', 'ca': '0.06426922', 'sa': '0.06426922', 'pa': '0.06426922', 'co': 1},
                      {'ra': '174354.86', 'ca': '0.00071899', 'sa': '0.00071899', 'pa': '0.00071899', 'co': 1},
                      {'ra': '174800.00', 'ca': '0.00700000', 'sa': '0.00700000', 'pa': '0.00700000', 'co': 1},
                      {'ra': '174800.01', 'ca': '0.03870294', 'sa': '0.03870294', 'pa': '0.03870294', 'co': 1}],
              'timestamp': '1644899039172',
              'seqNo': '95486654'}}

push = {'action': 'push',
        'topic': 'trading/orderbook-limited/btc-pln/10',
        'message': {'changes': [
            {'marketCode': 'BTC-PLN', 'entryType': 'Buy', 'rate': '173048.05', 'action': 'update', 'state': {'ra': '173048.05', 'ca': '0.18791', 'sa': '0.18791', 'pa': '0.18791', 'co': 1}},
            {'marketCode': 'BTC-PLN', 'entryType': 'Buy', 'rate': '174354.86', 'action': 'update', 'state': {'ra': '174354.86', 'ca': '0.07113621', 'sa': '0.07113621', 'pa': '0.07113621', 'co': 2}},
            {'marketCode': 'BTC-PLN', 'entryType': 'Buy', 'rate': '174354.84', 'action': 'remove'},
            {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '175728.68', 'action': 'update', 'state': {'ra': '175728.68', 'ca': '0.04474234', 'sa': '0.04474234', 'pa': '0.04474234', 'co': 1}},
            {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '176377.50', 'action': 'remove'}], 'timestamp': '1644899040733'},
        'timestamp': '1644899040733',
        'seqNo': 95486655}


# --- gemini websocket ob structure

snap = {'type': 'update',
        'eventId': 89156281495,
        'socket_sequence': 0,
        'events': [
            # irrational orders on the beggining, like BTC for 0.01 USD
            {'type': 'change', 'reason': 'initial', 'price': '0.01', 'delta': '125651', 'remaining': '125651', 'side': 'bid'},
                   {'type': 'change', 'reason': 'initial', 'price': '0.02', 'delta': '4284', 'remaining': '4284', 'side': 'bid'},
                   {'type': 'change', 'reason': 'initial', 'price': '0.03', 'delta': '1003275.99999999', 'remaining': '1003275.99999999', 'side': 'bid'},
                   {'type': 'change', 'reason': 'initial', 'price': '0.04', 'delta': '1971.5', 'remaining': '1971.5', 'side': 'bid'},
                   {'type': 'change', 'reason': 'initial', 'price': '0.05', 'delta': '1176.59999999', 'remaining': '1176.59999999', 'side': 'bid'},
            # next part, price 30% less than market, also useless for analysis
                   {'type': 'change', 'reason': 'initial', 'price': '33152.02', 'delta': '0.03040478', 'remaining': '0.03040478', 'side': 'bid'},
                   {'type': 'change', 'reason': 'initial', 'price': '33156.23', 'delta': '0.00096024', 'remaining': '0.00096024', 'side': 'bid'},
                   {'type': 'change', 'reason': 'initial', 'price': '33160.00', 'delta': '0.00030156', 'remaining': '0.00030156', 'side': 'bid'},
                   {'type': 'change', 'reason': 'initial', 'price': '33161.00', 'delta': '0.01507795', 'remaining': '0.01507795', 'side': 'bid'},
                   {'type': 'change', 'reason': 'initial', 'price': '33164.39', 'delta': '0.00020192', 'remaining': '0.00020192', 'side': 'bid'},
                   {'type': 'change', 'reason': 'initial', 'price': '33164.42', 'delta': '0.00092802', 'remaining': '0.00092802', 'side': 'bid'},
                   {'type': 'change', 'reason': 'initial', 'price': '33166.44', 'delta': '0.007', 'remaining': '0.007', 'side': 'bid'},
                   {'type': 'change', 'reason': 'initial', 'price': '33172.00', 'delta': '0.00075364', 'remaining': '0.00075364', 'side': 'bid'},
                   {'type': 'change', 'reason': 'initial', 'price': '33177.78', 'delta': '0.00447136', 'remaining': '0.00447136', 'side': 'bid'},
                   {'type': 'change', 'reason': 'initial', 'price': '33180.00', 'delta': '0.00030138', 'remaining': '0.00030138', 'side': 'bid'},
            # the higjhest [eg. 25 offers ] on the top of bids and the same for asks
                   {'type': 'change', 'reason': 'initial', 'price': '43501.85', 'delta': '0.14', 'remaining': '0.14', 'side': 'bid'},
                   {'type': 'change', 'reason': 'initial', 'price': '43506.54', 'delta': '0.14', 'remaining': '0.14', 'side': 'bid'},
                   {'type': 'change', 'reason': 'initial', 'price': '43508.49', 'delta': '1', 'remaining': '1', 'side': 'bid'},
                   {'type': 'change', 'reason': 'initial', 'price': '43509.90', 'delta': '0.00016698', 'remaining': '0.00016698', 'side': 'bid'},
                   {'type': 'change', 'reason': 'initial', 'price': '43510.95', 'delta': '0.14', 'remaining': '0.14', 'side': 'bid'},
                   {'type': 'change', 'reason': 'initial', 'price': '43515.46', 'delta': '0.22967248', 'remaining': '0.22967248', 'side': 'bid'},
                   {'type': 'change', 'reason': 'initial', 'price': '43517.22', 'delta': '0.14', 'remaining': '0.14', 'side': 'bid'},
                   {'type': 'change', 'reason': 'initial', 'price': '43518.09', 'delta': '0.03512993', 'remaining': '0.03512993', 'side': 'bid'},
                   {'type': 'change', 'reason': 'initial', 'price': '43518.26', 'delta': '0.22966027', 'remaining': '0.22966027', 'side': 'bid'},
                   {'type': 'change', 'reason': 'initial', 'price': '43519.70', 'delta': '0.64314', 'remaining': '0.64314', 'side': 'bid'},
                   {'type': 'change', 'reason': 'initial', 'price': '43519.92', 'delta': '0.00049115', 'remaining': '0.00049115', 'side': 'bid'},
                   {'type': 'change', 'reason': 'initial', 'price': '43520.68', 'delta': '0.0002369', 'remaining': '0.0002369', 'side': 'bid'},
                   {'type': 'change', 'reason': 'initial', 'price': '43521.58', 'delta': '0.14', 'remaining': '0.14', 'side': 'bid'},
                   {'type': 'change', 'reason': 'initial', 'price': '43521.68', 'delta': '0.1623407', 'remaining': '0.1623407', 'side': 'bid'},
                   {'type': 'change', 'reason': 'initial', 'price': '43522.73', 'delta': '0.06889815', 'remaining': '0.06889815', 'side': 'bid'},
                   {'type': 'change', 'reason': 'initial', 'price': '43523.95', 'delta': '0.00171099', 'remaining': '0.00171099', 'side': 'bid'},
                   {'type': 'change', 'reason': 'initial', 'price': '43526.50', 'delta': '0.14', 'remaining': '0.14', 'side': 'bid'},
                   {'type': 'change', 'reason': 'initial', 'price': '43526.60', 'delta': '0.21540238', 'remaining': '0.21540238', 'side': 'bid'},
                   {'type': 'change', 'reason': 'initial', 'price': '43530.47', 'delta': '0.05145', 'remaining': '0.05145', 'side': 'bid'},
                   {'type': 'change', 'reason': 'initial', 'price': '43531.66', 'delta': '0.0459321', 'remaining': '0.0459321', 'side': 'bid'},
                   {'type': 'change', 'reason': 'initial', 'price': '43533.58', 'delta': '0.02340187', 'remaining': '0.02340187', 'side': 'bid'},



                   {'type': 'change', 'reason': 'initial', 'price': '43537.77', 'delta': '0.1728643', 'remaining': '0.1728643', 'side': 'ask'},
                   {'type': 'change', 'reason': 'initial', 'price': '43542.22', 'delta': '0.0367574', 'remaining': '0.0367574', 'side': 'ask'},
                   {'type': 'change', 'reason': 'initial', 'price': '43542.23', 'delta': '0.35', 'remaining': '0.35', 'side': 'ask'},
                   {'type': 'change', 'reason': 'initial', 'price': '43542.32', 'delta': '0.14', 'remaining': '0.14', 'side': 'ask'},
                   {'type': 'change', 'reason': 'initial', 'price': '43544.05', 'delta': '0.03468363', 'remaining': '0.03468363', 'side': 'ask'},
                   {'type': 'change', 'reason': 'initial', 'price': '43550.97', 'delta': '0.14', 'remaining': '0.14', 'side': 'ask'},
                   {'type': 'change', 'reason': 'initial', 'price': '43556.25', 'delta': '0.14', 'remaining': '0.14', 'side': 'ask'},
                   {'type': 'change', 'reason': 'initial', 'price': '43557.91', 'delta': '0.05145', 'remaining': '0.05145', 'side': 'ask'},
                   {'type': 'change', 'reason': 'initial', 'price': '43558.68', 'delta': '1', 'remaining': '1', 'side': 'ask'},
                   {'type': 'change', 'reason': 'initial', 'price': '43559.06', 'delta': '0.06889977', 'remaining': '0.06889977', 'side': 'ask'},
                   {'type': 'change', 'reason': 'initial', 'price': '43559.59', 'delta': '0.045933', 'remaining': '0.045933', 'side': 'ask'},
                   {'type': 'change', 'reason': 'initial', 'price': '43561.83', 'delta': '0.14', 'remaining': '0.14', 'side': 'ask'},
                   {'type': 'change', 'reason': 'initial', 'price': '43563.42', 'delta': '0.045923', 'remaining': '0.045923', 'side': 'ask'},
                   {'type': 'change', 'reason': 'initial', 'price': '43565.39', 'delta': '0.22965639', 'remaining': '0.22965639', 'side': 'ask'},
                   {'type': 'change', 'reason': 'initial', 'price': '43566.25', 'delta': '0.14', 'remaining': '0.14', 'side': 'ask'},
                   {'type': 'change', 'reason': 'initial', 'price': '43567.36', 'delta': '0.045933', 'remaining': '0.045933', 'side': 'ask'},
                   {'type': 'change', 'reason': 'initial', 'price': '43568.56', 'delta': '0.28707827', 'remaining': '0.28707827', 'side': 'ask'},
                   {'type': 'change', 'reason': 'initial', 'price': '43568.68', 'delta': '0.64314', 'remaining': '0.64314', 'side': 'ask'},
                   {'type': 'change', 'reason': 'initial', 'price': '43570.72', 'delta': '0.14', 'remaining': '0.14', 'side': 'ask'},
                   {'type': 'change', 'reason': 'initial', 'price': '43575.08', 'delta': '0.14', 'remaining': '0.14', 'side': 'ask'},
                   {'type': 'change', 'reason': 'initial', 'price': '43575.10', 'delta': '0.341', 'remaining': '0.341', 'side': 'ask'},
                   {'type': 'change', 'reason': 'initial', 'price': '43577.86', 'delta': '0.34449075', 'remaining': '0.34449075', 'side': 'ask'},
                   {'type': 'change', 'reason': 'initial', 'price': '43581.43', 'delta': '0.14', 'remaining': '0.14', 'side': 'ask'}

        ]}


l = snap['events']

sides = [i['side'] for i in l]

# list.index() search for the first index that match to condition
# print(len(sides))
# print(sides.index('bid'))
# print(sides.index('ask'))


snap_2 = {'type': 'l2_updates', 'symbol': 'BTCUSD',
          'changes': [['buy', '43543.06', '0.03675442'],
                      ['buy', '43543.05', '0.02296197'],
                      ['buy', '43542.05', '0.35'],
                      ['buy', '43542.04', '0.14'],
                      ['buy', '43538.59', '0.04592445'],
                      ['buy', '43536.40', '0.35'],
                      ['buy', '43536.25', '0.14'],
                      ['buy', '43532.91', '0.04596'],
                      ['buy', '43531.84', '0.2176373'],
                      ['buy', '43531.74', '0.14'],
                      ['buy', '43528.93', '0.06888818'],
                      ['buy', '43527.76', '0.16081914'],
                      ['buy', '43527.66', '0.04179107'],
                      ['buy', '43527.30', '0.14'],
                      ['buy', '43526.17', '0.22961728'],
                      ['buy', '43523.14', '0.22963343'],
                      ['buy', '43521.49', '0.14'],
                      ['buy', '43519.74', '0.57449'],
                      ['buy', '43516.90', '0.14'],
                      ['buy', '43514.28', '0.2870589'],
                      ['buy', '43513.70', '0.341'], ['buy', '43510.93', '0.14'], ['buy', '43510.29', '0.045851'], ['buy', '43507.11', '1'], ['buy', '43506.25', '0.14'], ['buy', '43506.21', '0.00044022'], ['buy', '43505.18', '0.0007769'], ['buy', '43503.89', '0.34447947'], ['buy', '43502.49', '0.039183'], ['buy', '43501.60', '0.14'], ['buy', '43498.71', '0.00197254'], ['buy', '43497.06', '0.14'], ['buy', '43496.00', '1'], ['buy', '43493.46', '0.00206468'], ['buy', '43491.00', '1'], ['buy', '43489.97', '0.0002557'], ['buy', '43489.66', '0.14'], ['buy', '43488.04', '0.00059805'], ['buy', '43487.17', '0.0004599'], ['buy', '43486.40', '0.00126419'], ['buy', '43484.76', '0.14'], ['buy', '43483.21', '0.23713988'], ['buy', '43482.00', '1.98'], ['buy', '43481.11', '0.00240595'], ['buy', '43478.09', '0.4592614'], ['buy', '43477.60', '0.6569'], ['buy', '43473.37', '0.14'], ['buy', '43471.20', '0.00042227'], ['buy', '43469.03', '1.43622'], ['buy', '43468.74', '0.0002198'], ['buy', '43468.61', '0.00093311'], ['buy', '43468.29', '0.14'], ['buy', '43467.05', '0.519'],
                      ['sell', '43554.70', '0.03675442'],
                      ['sell', '43554.71', '0.04'],
                      ['sell', '43554.72', '0.14'],
                      ['sell', '43559.73', '0.14'],
                      ['sell', '43560.60', '1'], ['sell', '43564.12', '0.14'], ['sell', '43564.17', '0.06888818'], ['sell', '43565.50', '0.045931'], ['sell', '43565.69', '0.045936'], ['sell', '43565.96', '0.04596'], ['sell', '43567.60', '0.00070888'], ['sell', '43569.11', '0.14'], ['sell', '43571.37', '0.28704179'], ['sell', '43573.47', '0.14'], ['sell', '43574.57', '0.28702452'], ['sell', '43576.17', '0.045936'], ['sell', '43577.85', '0.12989788'], ['sell', '43579.13', '0.57449'], ['sell', '43581.16', '0.14'], ['sell', '43585.52', '0.14'], ['sell', '43587.08', '0.34448712'], ['sell', '43590.08', '0.14'], ['sell', '43590.80', '0.341'], ['sell', '43590.95', '0.34443699'], ['sell', '43597.54', '0.14'], ['sell', '43603.20', '0.6569'], ['sell', '43604.16', '0.596'], ['sell', '43604.95', '0.14'], ['sell', '43605.62', '0.45940759'], ['sell', '43609.31', '0.14'], ['sell', '43626.75', '1.43622'], ['sell', '43632.59', '0.00070782'], ['sell', '43636.00', '0.02'], ['sell', '43650.08', '15.412'], ['sell', '43651.80', '1.3458'], ['sell', '43661.89', '4.821'], ['sell', '43662.75', '0.51072442'], ['sell', '43666.55', '0.46008462'], ['sell', '43680.69', '4.19009261'], ['sell', '43686.32', '0.00024'], ['sell', '43697.67', '0.00070677'], ['sell', '43705.71', '8.97635'], ['sell', '43708.72', '0.00034722'], ['sell', '43719.76', '0.00200365'], ['sell', '43728.55', '0.00200346'], ['sell', '43732.26', '0.00034722'], ['sell', '43736.16', '0.60621451'], ['sell', '43741.36', '0.00021967'], ['sell', '43750.00', '0.025'], ['sell', '43753.93', '17.7'], ['sell', '43760.45', '0.00001742'], ['sell', '43762.85', '0.00070571'], ['sell', '43773.18', '6.8184'], ['sell', '43785.53', '0.00012'], ['sell', '43787.02', '0.00034722'], ['sell', '43789.63', '0.00081749'], ['sell', '43794.80', '0.00364049'], ['sell', '43795.77', '0.11112651'], ['sell', '43796.13', '0.00034722'], ['sell', '43800.00', '0.22819635'], ['sell', '43801.00', '0.0023'], ['sell', '43807.84', '0.00005322'], ['sell', '43807.91', '0.0000971'], ['sell', '43809.43', '0.00009521'], ['sell', '43811.45', '0.00246397'], ['sell', '43820.11', '0.00744275'], ['sell', '43825.24', '0.00109207'], ['sell', '43828.13', '0.00070466'], ['sell', '43833.15', '0.00022616'], ['sell', '43833.77', '0.00154541'], ['sell', '43836.69', '0.00034722'], ['sell', '43847.73', '0.00005039'], ['sell', '43850.00', '0.01977581'], ['sell', '43860.23', '0.00228814'], ['sell', '43866.38', '0.00073074'], ['sell', '43871.63', '0.00199693']]}


l_2 = snap_2['changes']


sides = [i[0] for i in l_2]

print(sides.index('buy'))
print(sides.index('sell'))