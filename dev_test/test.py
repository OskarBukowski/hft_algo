######
# In this script I have the proxy response with up to 300 lines of orderbook
# I need to create the async loop that will receive this data every few miliseconds
#####


import json
import datetime

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
            {'marketCode': 'BTC-PLN', 'entryType': 'Buy', 'rate': '173048.05', 'action': 'update',
             'state': {'ra': '173048.05', 'ca': '0.18791', 'sa': '0.18791', 'pa': '0.18791', 'co': 1}},
            {'marketCode': 'BTC-PLN', 'entryType': 'Buy', 'rate': '174354.86', 'action': 'update',
             'state': {'ra': '174354.86', 'ca': '0.07113621', 'sa': '0.07113621', 'pa': '0.07113621', 'co': 2}},
            {'marketCode': 'BTC-PLN', 'entryType': 'Buy', 'rate': '174354.84', 'action': 'remove'},
            {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '175728.68', 'action': 'update',
             'state': {'ra': '175728.68', 'ca': '0.04474234', 'sa': '0.04474234', 'pa': '0.04474234', 'co': 1}},
            {'marketCode': 'BTC-PLN', 'entryType': 'Sell', 'rate': '176377.50', 'action': 'remove'}],
            'timestamp': '1644899040733'},
        'timestamp': '1644899040733',
        'seqNo': 95486655}

# --- gemini websocket ob structure

snap = {'type': 'update',
        'eventId': 89156281495,
        'socket_sequence': 0,
        'events': [
            # irrational orders on the beginning, like BTC for 0.01 USD
            {'type': 'change', 'reason': 'initial', 'price': '0.01', 'delta': '125651', 'remaining': '125651',
             'side': 'bid'},
            {'type': 'change', 'reason': 'initial', 'price': '0.02', 'delta': '4284', 'remaining': '4284',
             'side': 'bid'},
            {'type': 'change', 'reason': 'initial', 'price': '0.03', 'delta': '1003275.99999999',
             'remaining': '1003275.99999999', 'side': 'bid'},
            {'type': 'change', 'reason': 'initial', 'price': '0.04', 'delta': '1971.5', 'remaining': '1971.5',
             'side': 'bid'},
            {'type': 'change', 'reason': 'initial', 'price': '0.05', 'delta': '1176.59999999',
             'remaining': '1176.59999999', 'side': 'bid'},
            # next part, price 30% less than market, also useless for analysis
            {'type': 'change', 'reason': 'initial', 'price': '33152.02', 'delta': '0.03040478',
             'remaining': '0.03040478', 'side': 'bid'},
            {'type': 'change', 'reason': 'initial', 'price': '33156.23', 'delta': '0.00096024',
             'remaining': '0.00096024', 'side': 'bid'},
            {'type': 'change', 'reason': 'initial', 'price': '33160.00', 'delta': '0.00030156',
             'remaining': '0.00030156', 'side': 'bid'},
            {'type': 'change', 'reason': 'initial', 'price': '33161.00', 'delta': '0.01507795',
             'remaining': '0.01507795', 'side': 'bid'},
            {'type': 'change', 'reason': 'initial', 'price': '33164.39', 'delta': '0.00020192',
             'remaining': '0.00020192', 'side': 'bid'},
            {'type': 'change', 'reason': 'initial', 'price': '33164.42', 'delta': '0.00092802',
             'remaining': '0.00092802', 'side': 'bid'},
            {'type': 'change', 'reason': 'initial', 'price': '33166.44', 'delta': '0.007', 'remaining': '0.007',
             'side': 'bid'},
            {'type': 'change', 'reason': 'initial', 'price': '33172.00', 'delta': '0.00075364',
             'remaining': '0.00075364', 'side': 'bid'},
            {'type': 'change', 'reason': 'initial', 'price': '33177.78', 'delta': '0.00447136',
             'remaining': '0.00447136', 'side': 'bid'},
            {'type': 'change', 'reason': 'initial', 'price': '33180.00', 'delta': '0.00030138',
             'remaining': '0.00030138', 'side': 'bid'},
            # the highest [eg. 25 offers ] on the top of bids and the same for asks
            {'type': 'change', 'reason': 'initial', 'price': '43501.85', 'delta': '0.14', 'remaining': '0.14',
             'side': 'bid'},
            {'type': 'change', 'reason': 'initial', 'price': '43506.54', 'delta': '0.14', 'remaining': '0.14',
             'side': 'bid'},
            {'type': 'change', 'reason': 'initial', 'price': '43508.49', 'delta': '1', 'remaining': '1', 'side': 'bid'},
            {'type': 'change', 'reason': 'initial', 'price': '43509.90', 'delta': '0.00016698',
             'remaining': '0.00016698', 'side': 'bid'},
            {'type': 'change', 'reason': 'initial', 'price': '43510.95', 'delta': '0.14', 'remaining': '0.14',
             'side': 'bid'},
            {'type': 'change', 'reason': 'initial', 'price': '43515.46', 'delta': '0.22967248',
             'remaining': '0.22967248', 'side': 'bid'},
            {'type': 'change', 'reason': 'initial', 'price': '43517.22', 'delta': '0.14', 'remaining': '0.14',
             'side': 'bid'},
            {'type': 'change', 'reason': 'initial', 'price': '43518.09', 'delta': '0.03512993',
             'remaining': '0.03512993', 'side': 'bid'},
            {'type': 'change', 'reason': 'initial', 'price': '43518.26', 'delta': '0.22966027',
             'remaining': '0.22966027', 'side': 'bid'},
            {'type': 'change', 'reason': 'initial', 'price': '43519.70', 'delta': '0.64314', 'remaining': '0.64314',
             'side': 'bid'},
            {'type': 'change', 'reason': 'initial', 'price': '43519.92', 'delta': '0.00049115',
             'remaining': '0.00049115', 'side': 'bid'},
            {'type': 'change', 'reason': 'initial', 'price': '43520.68', 'delta': '0.0002369', 'remaining': '0.0002369',
             'side': 'bid'},
            {'type': 'change', 'reason': 'initial', 'price': '43521.58', 'delta': '0.14', 'remaining': '0.14',
             'side': 'bid'},
            {'type': 'change', 'reason': 'initial', 'price': '43521.68', 'delta': '0.1623407', 'remaining': '0.1623407',
             'side': 'bid'},
            {'type': 'change', 'reason': 'initial', 'price': '43522.73', 'delta': '0.06889815',
             'remaining': '0.06889815', 'side': 'bid'},
            {'type': 'change', 'reason': 'initial', 'price': '43523.95', 'delta': '0.00171099',
             'remaining': '0.00171099', 'side': 'bid'},
            {'type': 'change', 'reason': 'initial', 'price': '43526.50', 'delta': '0.14', 'remaining': '0.14',
             'side': 'bid'},
            {'type': 'change', 'reason': 'initial', 'price': '43526.60', 'delta': '0.21540238',
             'remaining': '0.21540238', 'side': 'bid'},
            {'type': 'change', 'reason': 'initial', 'price': '43530.47', 'delta': '0.05145', 'remaining': '0.05145',
             'side': 'bid'},
            {'type': 'change', 'reason': 'initial', 'price': '43531.66', 'delta': '0.0459321', 'remaining': '0.0459321',
             'side': 'bid'},
            {'type': 'change', 'reason': 'initial', 'price': '43533.58', 'delta': '0.02340187',
             'remaining': '0.02340187', 'side': 'bid'},

            {'type': 'change', 'reason': 'initial', 'price': '43537.77', 'delta': '0.1728643', 'remaining': '0.1728643',
             'side': 'ask'},
            {'type': 'change', 'reason': 'initial', 'price': '43542.22', 'delta': '0.0367574', 'remaining': '0.0367574',
             'side': 'ask'},
            {'type': 'change', 'reason': 'initial', 'price': '43542.23', 'delta': '0.35', 'remaining': '0.35',
             'side': 'ask'},
            {'type': 'change', 'reason': 'initial', 'price': '43542.32', 'delta': '0.14', 'remaining': '0.14',
             'side': 'ask'},
            {'type': 'change', 'reason': 'initial', 'price': '43544.05', 'delta': '0.03468363',
             'remaining': '0.03468363', 'side': 'ask'},
            {'type': 'change', 'reason': 'initial', 'price': '43550.97', 'delta': '0.14', 'remaining': '0.14',
             'side': 'ask'},
            {'type': 'change', 'reason': 'initial', 'price': '43556.25', 'delta': '0.14', 'remaining': '0.14',
             'side': 'ask'},
            {'type': 'change', 'reason': 'initial', 'price': '43557.91', 'delta': '0.05145', 'remaining': '0.05145',
             'side': 'ask'},
            {'type': 'change', 'reason': 'initial', 'price': '43558.68', 'delta': '1', 'remaining': '1', 'side': 'ask'},
            {'type': 'change', 'reason': 'initial', 'price': '43559.06', 'delta': '0.06889977',
             'remaining': '0.06889977', 'side': 'ask'},
            {'type': 'change', 'reason': 'initial', 'price': '43559.59', 'delta': '0.045933', 'remaining': '0.045933',
             'side': 'ask'},
            {'type': 'change', 'reason': 'initial', 'price': '43561.83', 'delta': '0.14', 'remaining': '0.14',
             'side': 'ask'},
            {'type': 'change', 'reason': 'initial', 'price': '43563.42', 'delta': '0.045923', 'remaining': '0.045923',
             'side': 'ask'},
            {'type': 'change', 'reason': 'initial', 'price': '43565.39', 'delta': '0.22965639',
             'remaining': '0.22965639', 'side': 'ask'},
            {'type': 'change', 'reason': 'initial', 'price': '43566.25', 'delta': '0.14', 'remaining': '0.14',
             'side': 'ask'},
            {'type': 'change', 'reason': 'initial', 'price': '43567.36', 'delta': '0.045933', 'remaining': '0.045933',
             'side': 'ask'},
            {'type': 'change', 'reason': 'initial', 'price': '43568.56', 'delta': '0.28707827',
             'remaining': '0.28707827', 'side': 'ask'},
            {'type': 'change', 'reason': 'initial', 'price': '43568.68', 'delta': '0.64314', 'remaining': '0.64314',
             'side': 'ask'},
            {'type': 'change', 'reason': 'initial', 'price': '43570.72', 'delta': '0.14', 'remaining': '0.14',
             'side': 'ask'},
            {'type': 'change', 'reason': 'initial', 'price': '43575.08', 'delta': '0.14', 'remaining': '0.14',
             'side': 'ask'},
            {'type': 'change', 'reason': 'initial', 'price': '43575.10', 'delta': '0.341', 'remaining': '0.341',
             'side': 'ask'},
            {'type': 'change', 'reason': 'initial', 'price': '43577.86', 'delta': '0.34449075',
             'remaining': '0.34449075', 'side': 'ask'},
            {'type': 'change', 'reason': 'initial', 'price': '43581.43', 'delta': '0.14', 'remaining': '0.14',
             'side': 'ask'}

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
                      ['buy', '43513.70', '0.341'], ['buy', '43510.93', '0.14'], ['buy', '43510.29', '0.045851'],
                      ['buy', '43507.11', '1'], ['buy', '43506.25', '0.14'], ['buy', '43506.21', '0.00044022'],
                      ['buy', '43505.18', '0.0007769'], ['buy', '43503.89', '0.34447947'],
                      ['buy', '43502.49', '0.039183'], ['buy', '43501.60', '0.14'], ['buy', '43498.71', '0.00197254'],
                      ['buy', '43497.06', '0.14'], ['buy', '43496.00', '1'], ['buy', '43493.46', '0.00206468'],
                      ['buy', '43491.00', '1'], ['buy', '43489.97', '0.0002557'], ['buy', '43489.66', '0.14'],
                      ['buy', '43488.04', '0.00059805'], ['buy', '43487.17', '0.0004599'],
                      ['buy', '43486.40', '0.00126419'], ['buy', '43484.76', '0.14'], ['buy', '43483.21', '0.23713988'],
                      ['buy', '43482.00', '1.98'], ['buy', '43481.11', '0.00240595'], ['buy', '43478.09', '0.4592614'],
                      ['buy', '43477.60', '0.6569'], ['buy', '43473.37', '0.14'], ['buy', '43471.20', '0.00042227'],
                      ['buy', '43469.03', '1.43622'], ['buy', '43468.74', '0.0002198'],
                      ['buy', '43468.61', '0.00093311'], ['buy', '43468.29', '0.14'], ['buy', '43467.05', '0.519'],
                      ['sell', '43554.70', '0.03675442'],
                      ['sell', '43554.71', '0.04'],
                      ['sell', '43554.72', '0.14'],
                      ['sell', '43559.73', '0.14'],
                      ['sell', '43560.60', '1'], ['sell', '43564.12', '0.14'], ['sell', '43564.17', '0.06888818'],
                      ['sell', '43565.50', '0.045931'], ['sell', '43565.69', '0.045936'],
                      ['sell', '43565.96', '0.04596'], ['sell', '43567.60', '0.00070888'], ['sell', '43569.11', '0.14'],
                      ['sell', '43571.37', '0.28704179'], ['sell', '43573.47', '0.14'],
                      ['sell', '43574.57', '0.28702452'], ['sell', '43576.17', '0.045936'],
                      ['sell', '43577.85', '0.12989788'], ['sell', '43579.13', '0.57449'], ['sell', '43581.16', '0.14'],
                      ['sell', '43585.52', '0.14'], ['sell', '43587.08', '0.34448712'], ['sell', '43590.08', '0.14'],
                      ['sell', '43590.80', '0.341'], ['sell', '43590.95', '0.34443699'], ['sell', '43597.54', '0.14'],
                      ['sell', '43603.20', '0.6569'], ['sell', '43604.16', '0.596'], ['sell', '43604.95', '0.14'],
                      ['sell', '43605.62', '0.45940759'], ['sell', '43609.31', '0.14'], ['sell', '43626.75', '1.43622'],
                      ['sell', '43632.59', '0.00070782'], ['sell', '43636.00', '0.02'], ['sell', '43650.08', '15.412'],
                      ['sell', '43651.80', '1.3458'], ['sell', '43661.89', '4.821'], ['sell', '43662.75', '0.51072442'],
                      ['sell', '43666.55', '0.46008462'], ['sell', '43680.69', '4.19009261'],
                      ['sell', '43686.32', '0.00024'], ['sell', '43697.67', '0.00070677'],
                      ['sell', '43705.71', '8.97635'], ['sell', '43708.72', '0.00034722'],
                      ['sell', '43719.76', '0.00200365'], ['sell', '43728.55', '0.00200346'],
                      ['sell', '43732.26', '0.00034722'], ['sell', '43736.16', '0.60621451'],
                      ['sell', '43741.36', '0.00021967'], ['sell', '43750.00', '0.025'], ['sell', '43753.93', '17.7'],
                      ['sell', '43760.45', '0.00001742'], ['sell', '43762.85', '0.00070571'],
                      ['sell', '43773.18', '6.8184'], ['sell', '43785.53', '0.00012'],
                      ['sell', '43787.02', '0.00034722'], ['sell', '43789.63', '0.00081749'],
                      ['sell', '43794.80', '0.00364049'], ['sell', '43795.77', '0.11112651'],
                      ['sell', '43796.13', '0.00034722'], ['sell', '43800.00', '0.22819635'],
                      ['sell', '43801.00', '0.0023'], ['sell', '43807.84', '0.00005322'],
                      ['sell', '43807.91', '0.0000971'], ['sell', '43809.43', '0.00009521'],
                      ['sell', '43811.45', '0.00246397'], ['sell', '43820.11', '0.00744275'],
                      ['sell', '43825.24', '0.00109207'], ['sell', '43828.13', '0.00070466'],
                      ['sell', '43833.15', '0.00022616'], ['sell', '43833.77', '0.00154541'],
                      ['sell', '43836.69', '0.00034722'], ['sell', '43847.73', '0.00005039'],
                      ['sell', '43850.00', '0.01977581'], ['sell', '43860.23', '0.00228814'],
                      ['sell', '43866.38', '0.00073074'], ['sell', '43871.63', '0.00199693']]}

l_2 = snap_2['changes']

sides = [i[0] for i in l_2]

# print(sides.index('buy'))
# print(sides.index('sell'))


r = [{'success': False, 'error': {'code': 200, 'message': 'Too many requests.'}},
     {'success': False, 'error': {'code': 200, 'message': 'Too many requests.'}},
     {'success': False, 'error': {'code': 200, 'message': 'Too many requests.'}},
     {'success': False, 'error': {'code': 200, 'message': 'Too many requests.'}},
     {'success': False, 'error': {'code': 200, 'message': 'Too many requests.'}},
     {'success': False, 'error': {'code': 200, 'message': 'Too many requests.'}},
     {'success': False, 'error': {'code': 200, 'message': 'Too many requests.'}},
    {'success': False, 'error': {'code': 200, 'message': 'Too many requests.'}}]



# print(b[0]['success'])  # check the status
# print(b[0]['payload']['bids'][0]['price'])  # first line bid price
# print(b[0]['payload']['bids'][0]['amount'])  # first line bid volume
# print(b[0]['payload']['bids'][1]['price'])  # second line bid price
# print(b[0]['payload']['bids'][1]['amount'])  # second line bid volume
#
# print(b[0]['payload']['asks'][0]['price'])  # first line ask price
# print(b[0]['payload']['asks'][0]['amount'])  # first line ask volume
# print(b[0]['payload']['asks'][1]['price'])  # second line ask price
# print(b[0]['payload']['asks'][1]['amount'])  # second line ask volume
#
# t = b[0]['payload']['updated_at'].replace("T", " ").split("+")[0]
# print(t)
#
# int(datetime.datetime.strptime(t, '%Y-%m-%d %H:%M:%S').timestamp())

import time

b = [[[40950, 1, 0.16730757],
      [40944, 1, 0.03620658],
      [40940, 2, 0.67928439],
      [40938, 2, 0.53603986],
      [40937, 2, 0.267765],
      [40936, 3, 1.15979815],
      [40935, 3, 0.003],
      [40934, 4, 0.186207],
      [40933, 5, 0.297249],
      [40932, 1, 0.05],
 [40962, 2, -0.80598452], [40963, 4, -1.7359], [40964, 1, -0.183218], [40965, 3, -0.96934206], [40966, 1, -0.7776], [40967, 1, -0.41249221], [40968, 3, -0.60000813], [40969, 2, -0.10492913], [40972, 3, -0.79980379], [40973, 2, -1.32750136], [40974, 1, -0.46405374], [40975, 1, -0.00037127], [40976, 3, -0.6495], [40977, 1, -0.24426798], [40978, 1, -0.183214], [40979, 3, -0.86779701], [40980, 3, -0.29458943]], [[2906.5, 1, 1.46571958], [2906.4, 1, 1.12960472], [2906.2, 3, 11.76744858], [2906.1, 1, 0.1719], [2906, 2, 4.55527932], [2905.9, 4, 9.58606066], [2905.8, 1, 3.0407], [2905.7, 2, 9.68368], [2905.6, 4, 3.61567903], [2905.5, 3, 5.70961447], [2905.4, 3, 19.66874687], [2905.3, 3, 20.60591826], [2905.2, 1, 2.58103], [2905.1, 5, 8.34931583], [2905, 1, 0.001], [2904.9, 1, 0.08176106], [2904.8, 3, 6.15443064], [2904.7, 1, 3.44135], [2904.6, 1, 32.55440863], [2904.5, 4, 8.04622175], [2904.4, 1, 8.38], [2904.2, 1, 17.20344], [2904.1, 4, 23.45111033], [2904, 3, 36.52639576], [2903.9, 2, 24.97517944], [2907.2, 1, -0.86033], [2907.3, 1, -1.72066], [2907.4, 1, -1.12411092], [2907.5, 1, -0.87564121], [2907.7, 3, -4.19433107], [2907.8, 1, -1.1265], [2907.9, 2, -12.3391], [2908, 3, -4.83514808], [2908.1, 2, -2.99406], [2908.2, 2, -8.8773], [2908.3, 2, -1.5], [2908.4, 2, -0.57789676], [2908.5, 1, -2.58086], [2908.6, 2, -4.76292552], [2908.8, 1, -2.69699781], [2908.9, 3, -7.45932417], [2909, 2, -8.737], [2909.1, 4, -8.26944477], [2909.2, 5, -2.743172], [2909.4, 1, -0.35940729], [2909.5, 1, -17.20673], [2909.6, 4, -5.1932425], [2909.7, 3, -3.88216799], [2909.8, 1, -8.60231], [2909.9, 2, -6.3779]], [[2.877e-05, 1, 146546860.5799], [2.876e-05, 5, 605473536.8310777], [2.875e-05, 4, 235615671.3824128], [2.873e-05, 7, 632179596.8239932], [2.872e-05, 1, 260570643.45350245], [2.871e-05, 8, 6205743893.22049], [2.87e-05, 1, 29406578.61602275], [2.868e-05, 1, 347474000], [2.862e-05, 1, 667219279], [2.861e-05, 1, 39183142.29241956], [2.859e-05, 2, 58779300.53214769], [2.858e-05, 1, 48989821.18040122], [2.853e-05, 2, 495417869], [2.852e-05, 2, 5224630080.940232], [2.84e-05, 1, 78389171.94495158], [2.835e-05, 1, 1000000], [2.833e-05, 2, 5098020865.685381], [2.822e-05, 1, 82984440], [2.818e-05, 1, 500000], [2.814e-05, 1, 98020865.68538119], [2.81e-05, 1, 12000000], [2.806e-05, 1, 42105947], [2.799e-05, 1, 117625038.82245743], [2.77e-05, 1, 343469.791105], [2.764e-05, 1, 446.80150107], [2.881e-05, 6, -409056271.3447269], [2.882e-05, 4, -570440017.6687222], [2.883e-05, 2, -131078001], [2.884e-05, 1, -104668863.82865515], [2.885e-05, 5, -628375270.7202504], [2.886e-05, 2, -429662619], [2.887e-05, 1, -19168885.50718855], [2.888e-05, 3, -518775431.9093869], [2.889e-05, 1, -168288347], [2.895e-05, 1, -133874648.84152983], [2.896e-05, 1, -48905060.03752065], [2.898e-05, 1, -4185792], [2.899e-05, 3, -1044994101.971125], [2.91e-05, 1, -77459049.61211596], [2.92e-05, 1, -97531331.66082281], [2.938e-05, 1, -96993779.951875], [2.951e-05, 1, -191460847.17089185], [2.952e-05, 1, -5000000000], [2.954e-05, 1, -13542325.37004], [2.956e-05, 1, -1671782.10753055], [2.957e-05, 1, -116743505.09740217], [2.967e-05, 1, -229955582.53638634], [2.968e-05, 1, -5000000000], [2.979e-05, 1, -5000000000], [2.985e-05, 1, -136200755.94696915]], [[87.997, 1, 479.77244571], [87.988, 1, 2.69080546], [87.984, 1, 204.3], [87.976, 1, 17.261], [87.953, 1, 5.90287336], [87.952, 1, 7.18], [87.951, 1, 17.4], [87.942, 1, 12.97466131], [87.941, 1, 111.5], [87.94, 1, 11.86130672], [87.939, 1, 5.38738457], [87.921, 1, 28.9327], [87.915, 1, 23.7423277], [87.913, 1, 29.1141], [87.912, 1, 335.7], [87.903, 1, 10], [87.896, 1, 11.86169412], [87.894, 1, 16.09558593], [87.884, 1, 113.56], [87.874, 1, 29.68098493], [87.863, 1, 76.8414], [87.862, 1, 23.748923], [87.856, 1, 40.7204], [87.855, 1, 28], [87.837, 1, 10.70774437], [88.097, 1, -111.5], [88.098, 1, -17.038], [88.122, 1, -56.792], [88.125, 1, -2.69080546], [88.127, 1, -30.2881], [88.13, 1, -17.312], [88.147, 1, -3.3], [88.158, 1, -28.1883], [88.161, 1, -4.62044901], [88.162, 1, -1.6370839], [88.164, 3, -67.15452961], [88.169, 1, -82.1714], [88.171, 1, -267.48807409], [88.188, 1, -78.2506], [88.189, 2, -2.94852712], [88.191, 1, -9.61213732], [88.205, 1, -68.37], [88.207, 1, -29.66879377], [88.215, 1, -0.4998874], [88.223, 1, -12.72069398], [88.228, 1, -0.5031224], [88.229, 1, -0.4833827], [88.236, 1, -0.27413938], [88.237, 1, -0.67115786], [88.24, 1, -0.4822579]], [[20.64, 2, 109.6808], [20.639, 1, 72.661], [20.635, 1, 2.2770887], [20.634, 2, 121.85004713], [20.628, 1, 47.1], [20.627, 1, 19.7326], [20.624, 1, 26.72025027], [20.623, 1, 0.5882595], [20.62, 1, 144.7], [20.619, 2, 560.0656], [20.615, 1, 49.30675479], [20.614, 1, 50.57110528], [20.613, 1, 242.148], [20.611, 1, 126.51339153], [20.61, 1, 7.3986392], [20.609, 1, 1.9404213], [20.608, 3, 46.87926422], [20.607, 1, 0.96423938], [20.604, 1, 54.81870605], [20.601, 2, 245.53165147], [20.6, 1, 101.2078223], [20.599, 1, 1.9853756], [20.598, 1, 1.91693901], [20.597, 1, 1.9497381], [20.595, 1, 2.4832588], [20.662, 1, -289.7], [20.667, 1, -19.35209104], [20.668, 1, -72.639], [20.673, 1, -73.1951], [20.678, 1, -9.50655568], [20.68, 1, -19.5107], [20.681, 1, -25.27329765], [20.682, 2, -93.11143766], [20.687, 1, -43.77297675], [20.69, 1, -40.84859705], [20.693, 2, -225.443252], [20.694, 1, -47.1], [20.7, 1, -1], [20.703, 1, -1.06653918], [20.704, 2, -244.04900926], [20.705, 1, -78.7], [20.715, 1, -54.46479607], [20.72, 2, -139.55530983], [20.722, 2, -12.34481882], [20.723, 1, -164.94067236], [20.724, 1, -157.5], [20.726, 2, -187.18233385], [20.727, 1, -164.94858414], [20.728, 1, -484.306], [20.733, 1, -1.60204252]], [[1.029, 2, 2941.7544], [1.0289, 2, 10588.3454], [1.0288, 4, 7492.96501735], [1.0287, 2, 2288.7939], [1.0286, 2, 2441.29205325], [1.0285, 3, 6202.38414772], [1.0284, 4, 6577.89037026], [1.0283, 2, 4393.50148551], [1.0282, 2, 3887.62844], [1.0281, 5, 6967.58974224], [1.028, 1, 513.92508083], [1.0279, 1, 856.09383994], [1.0278, 2, 1823.99017827], [1.0277, 2, 10804.70823315], [1.0276, 1, 20.01], [1.0275, 1, 300], [1.0274, 1, 16571], [1.0273, 2, 10244.96739119], [1.0272, 1, 9714.76], [1.0271, 1, 13452.53934378], [1.027, 2, 47293.44664029], [1.0269, 2, 4741.742328], [1.0268, 2, 4672.97582688], [1.0267, 1, 5956.85640396], [1.0266, 2, 6057.03661035], [1.0297, 1, -100], [1.0298, 1, -874.1133], [1.0299, 2, -2387.833], [1.03, 1, -9704], [1.0301, 4, -6866.93266142], [1.0302, 4, -5940.3154882], [1.0303, 2, -1515.98641564], [1.0304, 4, -6642.19362853], [1.0305, 1, -3756.0558], [1.0306, 1, -925.803], [1.0307, 3, -2026.44736842], [1.0308, 2, -651.61101831], [1.031, 2, -4585.62962959], [1.0311, 2, -8511.93894025], [1.0312, 3, -12679.2686451], [1.0313, 4, -3077.74688116], [1.0314, 3, -2421.2970412], [1.0315, 1, -5076.53389481], [1.0316, 2, -13624.13295069], [1.0317, 5, -27197.19517924], [1.0318, 3, -6797.63576298], [1.0319, 3, -7177.78851472], [1.0321, 3, -31436.61711543], [1.0322, 1, -1101.24424913], [1.0323, 6, -11911.0235143]], [[94.393, 1, 5.295], [94.383, 1, 22.7655], [94.38, 1, 15.893], [94.375, 1, 24.9235], [94.374, 1, 45.23782377], [94.359, 1, 24.0804], [94.355, 1, 113.9], [94.35, 1, 52.9766], [94.34, 1, 106.1], [94.336, 1, 16.40446135], [94.333, 1, 15.9204], [94.33, 1, 15.29], [94.326, 1, 75.6178], [94.311, 3, 118.2823402], [94.31, 1, 6.73802099], [94.308, 1, 5.53212409], [94.298, 1, 63.49], [94.292, 1, 74.5694], [94.29, 1, 191.14], [94.281, 1, 5.53876135], [94.275, 1, 23.1446], [94.274, 1, 110.8846], [94.265, 1, 185.7], [94.249, 1, 382.4], [94.246, 2, 44.26773736], [94.422, 1, -1.27376119], [94.423, 1, -3.2182063], [94.43, 1, -15.8935], [94.445, 1, -1.49946143], [94.447, 1, -5.296], [94.45, 1, -52.9781], [94.454, 1, -106.1], [94.455, 1, -22.9797], [94.46, 2, -22.29], [94.462, 2, -18.23396602], [94.469, 1, -23.9925], [94.47, 1, -26.4427], [94.486, 1, -5.53057359], [94.493, 1, -4.76], [94.494, 1, -11.0632348], [94.497, 1, -15.28847273], [94.498, 1, -4.46806254], [94.502, 1, -15.8415], [94.506, 1, -24.0676], [94.508, 1, -15.82400757], [94.509, 2, -5.52], [94.51, 1, -191.14], [94.513, 1, -6.63], [94.519, 1, -77.8904], [94.524, 1, -79.6404]], [[0.77138, 1, 132.92960627], [0.77137, 2, 2093.53384], [0.77136, 1, 3010.602], [0.77134, 1, 55.66857132], [0.77133, 2, 12046.3908], [0.77131, 1, 14499.9], [0.7713, 1, 935], [0.77129, 1, 1909.46731375], [0.77124, 1, 6480.5], [0.77122, 2, 4660.40060968], [0.7712, 2, 11963.71821028], [0.77119, 1, 12872], [0.77112, 1, 894.52975447], [0.77111, 1, 2825.9746], [0.7711, 1, 11691], [0.77105, 1, 11721.4633], [0.77102, 1, 9838.9745], [0.771, 1, 59.45053146], [0.77092, 1, 1090.83409291], [0.7709, 1, 2851.7952], [0.77088, 1, 1353.29271297], [0.77086, 1, 15000], [0.77077, 1, 2708.74539414], [0.77075, 1, 1000], [0.77071, 1, 1454.44545722], [0.77193, 3, -7225.66162], [0.77211, 1, -12872], [0.77212, 1, -6480.8], [0.77215, 1, -1988.74089812], [0.77219, 3, -5382.31483675], [0.7722, 1, -935], [0.77222, 1, -144.08465483], [0.77225, 1, -3731.03336124], [0.77232, 1, -249.0357189], [0.77237, 1, -975], [0.77239, 1, -462.58398522], [0.7724, 1, -1959.16622403], [0.77242, 2, -18086.413], [0.77243, 1, -608.71973967], [0.77244, 1, -307.71821028], [0.77246, 2, -5671.05129708], [0.77248, 1, -1000], [0.7725, 3, -39951.54941592], [0.77253, 1, -713.27404357], [0.77256, 2, -1522.38272851], [0.77257, 1, -2888.2962], [0.7726, 2, -31691], [0.77265, 1, -9387.0559], [0.77268, 1, -13480.37410052], [0.77269, 1, -1353.74510569]], [[15.729, 2, 120.367], [15.727, 1, 317.864], [15.726, 2, 50.92820103], [15.723, 1, 94.6484], [15.721, 1, 54.31636821], [15.719, 1, 15.1112009], [15.717, 3, 193.47963305], [15.714, 1, 81.27187652], [15.713, 1, 191.5], [15.712, 2, 682.65522627], [15.71, 1, 114.18509693], [15.709, 2, 228.02464209], [15.708, 2, 94.09629059], [15.707, 1, 43.1656859], [15.706, 1, 33.20489887], [15.705, 1, 66.39464514], [15.702, 2, 302.37648747], [15.701, 3, 347.35707476], [15.7, 2, 3737.106], [15.697, 1, 635.917], [15.693, 2, 470.87858701], [15.692, 1, 22.369339], [15.691, 1, 254.26119915], [15.689, 1, 1596.07325427], [15.687, 3, 1897.33], [15.74, 2, -107.698229], [15.741, 2, -955.207], [15.742, 2, -383.2030872], [15.743, 3, -446.89301038], [15.744, 1, -35.7791541], [15.745, 1, -164.8849], [15.746, 1, -15.1112009], [15.747, 1, -94.3344], [15.749, 1, -0.64088016], [15.75, 3, -215.27480663], [15.751, 1, -66.42076325], [15.754, 3, -70.30730192], [15.755, 3, -689.45424615], [15.758, 2, -265.67215973], [15.759, 1, -119.30118499], [15.76, 3, -104.20309375], [15.763, 1, -936], [15.764, 1, -25], [15.768, 1, -635.593], [15.769, 1, -170.43026427], [15.77, 2, -460.89371422], [15.774, 3, -805.96736564], [15.775, 1, -290.87858701], [15.778, 1, -635.182], [15.779, 1, -255.6453964]], [[117.04, 1, 12.8163], [117.03, 1, 24.6234], [117.02, 1, 4.84985073], [117.01, 4, 74.89283993], [116.99, 3, 105.56166], [116.98, 3, 82.34184451], [116.97, 3, 100.7458], [116.96, 6, 184.15451502], [116.94, 2, 27.32680078], [116.93, 3, 33.95074473], [116.92, 1, 53.6674], [116.91, 1, 24.0287], [116.9, 2, 29.80652543], [116.89, 3, 143.07426881], [116.88, 3, 103.706], [116.86, 1, 85.432], [116.85, 1, 17.84338121], [116.84, 1, 17.85475844], [116.83, 1, 31.4136771], [116.82, 1, 157.2], [116.79, 1, 156.30300666], [116.78, 3, 56.7515849], [116.77, 3, 510.8129872], [116.76, 4, 126.70019076], [116.74, 1, 0.814], [117.11, 3, -40.17497496], [117.12, 2, -0.3405002], [117.13, 4, -117.4167], [117.14, 1, -7.25970628], [117.16, 3, -51.88813993], [117.17, 3, -49.27239866], [117.18, 1, -28.0674], [117.19, 2, -0.2757716], [117.2, 1, -16.56199589], [117.21, 3, -37.68444531], [117.22, 3, -57.407], [117.23, 2, -81.686], [117.24, 2, -43.46519968], [117.25, 3, -92.44713551], [117.26, 3, -107.40374539], [117.27, 2, -111.51454921], [117.28, 1, -28], [117.29, 1, -152.87489151], [117.3, 2, -178.7379], [117.31, 2, -38.43401538], [117.32, 2, -157.5492978], [117.35, 1, -54.82008294], [117.36, 5, -85.06204043], [117.37, 1, -85.3969], [117.38, 2, -39.08179895]], [[18.233, 1, 32.31396107], [18.23, 1, 44.4198], [18.227, 1, 13.0534804], [18.226, 1, 7.67046222], [18.225, 2, 173.17135288], [18.224, 2, 308.8156], [18.222, 3, 969.4271], [18.221, 3, 86.56192896], [18.22, 1, 43.1209], [18.219, 1, 83.06771277], [18.218, 1, 28.64281233], [18.216, 2, 52.87908533], [18.211, 2, 77.27273604], [18.209, 2, 211.98662101], [18.207, 2, 189.97302896], [18.206, 1, 498], [18.203, 1, 143.22705623], [18.202, 2, 2400.6961], [18.201, 2, 171.92020705], [18.198, 1, 214.57973154], [18.196, 1, 548.72], [18.195, 1, 77.64436836], [18.192, 1, 221.29407274], [18.191, 2, 715.23488417], [18.187, 2, 238.76692901], [18.243, 1, -7.73324627], [18.246, 2, -3.00482346], [18.247, 2, -594.7829], [18.25, 1, -20], [18.251, 3, -240.6188804], [18.252, 2, -45.8435374], [18.253, 3, -265.24677254], [18.254, 3, -135.49466772], [18.255, 1, -388.40817295], [18.256, 1, -82.9485], [18.258, 1, -2.3388297], [18.259, 1, -2.2841643], [18.26, 1, -46.50313816], [18.261, 1, -2.2927243], [18.262, 2, -156.9989], [18.264, 1, -498], [18.265, 3, -55.0032128], [18.271, 2, -260.07852541], [18.272, 2, -889.5109], [18.274, 2, -406.70523026], [18.283, 1, -5.39], [18.284, 1, -732.81545176], [18.285, 1, -357.61203286], [18.286, 1, -124.00836841], [18.288, 2, -275.50270599]], [[51.419, 1, 47.9666], [51.418, 1, 47.3135], [51.408, 1, 33.13342384], [51.401, 1, 1], [51.399, 1, 16.83522787], [51.398, 2, 193.4022], [51.397, 3, 91.78196056], [51.396, 1, 55.1263926], [51.389, 1, 19.447], [51.383, 1, 66.12508042], [51.378, 2, 89.08312039], [51.377, 1, 319.3971], [51.375, 4, 258.68979036], [51.374, 1, 152.32636739], [51.371, 1, 116.2], [51.367, 1, 15.57407117], [51.366, 1, 88.31551945], [51.365, 2, 359.303112], [51.361, 1, 109.71988559], [51.36, 1, 20.31462466], [51.359, 1, 0.92], [51.358, 1, 40.6280461], [51.356, 1, 48.1751], [51.355, 1, 50], [51.349, 1, 58.23], [51.458, 1, -9.72], [51.459, 1, -48.1415], [51.461, 1, -4.49196056], [51.467, 2, -77.35614117], [51.469, 3, -167.66408], [51.471, 1, -32.55687394], [51.472, 1, -31.32], [51.474, 1, -58.16], [51.475, 1, -29.6], [51.479, 1, -19.445], [51.483, 1, -53.35012115], [51.485, 2, -68.66495099], [51.493, 2, -2114.4093], [51.494, 1, -63.99863928], [51.496, 1, -46.1721], [51.497, 1, -9.72966556], [51.498, 3, -318.6104], [51.499, 1, -152.33909787], [51.502, 1, -32.99121204], [51.505, 2, -132.06178615], [51.508, 1, -150.2328], [51.511, 1, -20.30906864], [51.512, 1, -50.7814621], [51.515, 1, -339], [51.517, 1, -15.52888742]], [[10.444, 2, 114.67489], [10.443, 1, 962.9], [10.442, 1, 143.572], [10.441, 2, 157.08633716], [10.44, 3, 527.21287144], [10.439, 2, 290.83125791], [10.438, 1, 27.1644639], [10.435, 3, 652.72619902], [10.434, 1, 40.74669585], [10.433, 1, 289.1], [10.431, 1, 99.95795926], [10.43, 1, 54.3289278], [10.428, 2, 1112.29689822], [10.427, 2, 688.536], [10.426, 1, 81.4933917], [10.425, 1, 793], [10.424, 2, 1679.192039], [10.422, 1, 108.65785559], [10.421, 1, 508.62691125], [10.418, 3, 1057.33268487], [10.417, 1, 162.98678339], [10.416, 1, 956.813], [10.411, 1, 92.97261543], [10.41, 1, 271.64463899], [10.409, 1, 1.020564], [10.458, 2, -216.6033], [10.459, 1, -143.548], [10.461, 3, -1179.01471951], [10.462, 2, -311.73567038], [10.463, 2, -192.3797], [10.465, 1, -13.5158168], [10.466, 2, -147.8925], [10.467, 3, -55.98195875], [10.469, 2, -508.73800261], [10.47, 2, -246.8886], [10.472, 2, -44.67503726], [10.473, 3, -486.0742686], [10.474, 1, -4.3262962], [10.475, 1, -4.1972494], [10.476, 3, -865.08095473], [10.479, 1, -55.242], [10.48, 2, -85.84495349], [10.483, 1, -403], [10.484, 3, -1125.80034547], [10.488, 2, -8.2584496], [10.489, 2, -186.95491618], [10.493, 3, -580.62796914], [10.494, 3, -1403.21720195], [10.495, 1, -793], [10.496, 1, -4.1972494]], [[0.14013, 1, 10703.6], [0.14011, 2, 77222.08658714], [0.1401, 2, 41757.03932089], [0.14009, 1, 10881.188], [0.14008, 3, 32167.09674344], [0.14006, 1, 22950.0445], [0.14005, 2, 56176.27072739], [0.14004, 1, 200], [0.14003, 3, 37685.41435481], [0.14002, 2, 71150], [0.14001, 1, 23959.7205], [0.14, 2, 19597.39865245], [0.13998, 1, 21405], [0.13997, 2, 11176.60638343], [0.13996, 1, 14157.97102322], [0.13991, 2, 172244.89562562], [0.1399, 1, 281.44052558], [0.13989, 1, 20225.67289032], [0.13985, 1, 280120.5], [0.13984, 2, 125538.7364265], [0.13983, 1, 32544.63500526], [0.13982, 1, 230869], [0.13976, 1, 236713.10471481], [0.13974, 1, 103116.20352082], [0.13973, 1, 71355], [0.14027, 4, -13953.20036027], [0.14028, 2, -46277.646], [0.1403, 1, -147.54045056], [0.14031, 1, -5273.44978013], [0.14032, 3, -86305.16440211], [0.14033, 3, -44759.98738851], [0.14034, 1, -23076.3074], [0.14037, 4, -40496.40257503], [0.14038, 2, -206836.86119412], [0.1404, 1, -3724.02478353], [0.14043, 1, -22595.5884], [0.14045, 2, -79725.97395838], [0.14046, 1, -91.41267618], [0.14047, 1, -35672], [0.14048, 1, -71341.1], [0.14049, 1, -180.04222772], [0.14052, 1, -14047.12244711], [0.14053, 1, -32544.63500526], [0.14054, 2, -96960.5364265], [0.14055, 2, -118180.07199436], [0.14056, 1, -71304.1], [0.14057, 1, -357.8897791], [0.14058, 2, -1032.6150771], [0.14059, 1, -20067.31778159], [0.1406, 2, -102591.61406439]], ['error', 10020, 'symbol: invalid'], [[1.6864, 1, 444.59], [1.6861, 1, 889.247], [1.6858, 1, 889.26], [1.6857, 1, 1548.0626704], [1.6856, 1, 309.67720914], [1.6855, 1, 2857.2825], [1.6854, 1, 4644.86077057], [1.6853, 1, 2927.6239], [1.6852, 1, 891.559], [1.6851, 1, 1238.73190498], [1.6849, 2, 2476.8421697], [1.6848, 1, 1545], [1.6847, 4, 4801.84369977], [1.6846, 1, 619.32895196], [1.6837, 1, 1678.84369977], [1.6835, 2, 4010.13829068], [1.6834, 1, 1833.26129922], [1.6833, 1, 5920.34], [1.6829, 2, 25165.815239], [1.6828, 1, 5153], [1.6822, 1, 3357.68739955], [1.6821, 1, 7803.72534332], [1.682, 1, 3357.68739955], [1.6817, 2, 2536.8674057], [1.6816, 1, 5925.34], [1.6877, 1, -295.80547], [1.6879, 1, -444.64], [1.6884, 1, -889.19], [1.6885, 1, -2829.5282], [1.6887, 1, -896.415], [1.6892, 3, -4183.6851], [1.6895, 1, -239.47038318], [1.6896, 2, -32493.13313992], [1.6898, 1, -309.4882572], [1.69, 1, -670.38013211], [1.6902, 1, -0.0008957], [1.6904, 2, -1624.18413847], [1.6907, 2, -4650.88275553], [1.6908, 1, -2964.33], [1.691, 1, -1545], [1.6913, 1, -30.41595], [1.6914, 2, -2227.29751373], [1.6915, 2, -2913.53453351], [1.6916, 1, -29.8906112], [1.6918, 1, -24.5589951], [1.6919, 1, -7.21252084], [1.692, 1, -8729.57588652], [1.6921, 2, -5136.92549541], [1.6922, 1, -29.7626995], [1.6926, 1, -30.3374603]]]

print([i for i in b[0] if i[2] > 0][0][0])  # first bid price
print([i for i in b[0] if i[2] > 0][0][2])  # first bid volume



print([i for i in b[0] if i[2] < 0][0][0])  # first ask price
print([i for i in b[0] if i[2] < 0][0][2])  # first ask volume


print(round(time.time() * 1000))  # current timestamp in ms

