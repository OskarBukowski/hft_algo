from requests import Session
import requests
import asyncio
import time
import ast
import json
from admin_tools import connection, logger_conf

# response model for ob

# {
#   "error": 0,
#   "result": [
#     [
#       1, // order id
#       1529453033, // timestamp
#       997.50, // volume [ currency ]
#       10000.00, // rate
#       0.09975000 // amount
#     ]
#   ]
# }

# resp = requests.get('https://api.bitkub.com/api/market/bids?sym=THB_BTC&lmt=5')
# resp2 = requests.get('https://api.bitkub.com/api/market/asks?sym=THB_BTC&lmt=5')
#
# print("timestamp:", ast.literal_eval(resp.text)['result'][0][1])
# print("price:", ast.literal_eval(resp.text)['result'][0][3])
# print("volume:", ast.literal_eval(resp.text)['result'][0][4])
#
# print(resp.text)
# print(resp2.text)

async def main():
    # cursor = connection()
    s = Session()
    s.max_redirects = 1000

    exchange_spec_dict = json.load(open('exchanges'))
    mapped_currency = exchange_spec_dict['currency_mapping'][1]['symbols']
    rest_url = exchange_spec_dict['source'][1]['rest_url']

    url_dict = {
        'btcthb': [
                f"{rest_url}/market/bids?sym={mapped_currency['btcthb']}&lmt=5",
                f"{rest_url}/market/asks?sym={mapped_currency['btcthb']}&lmt=5"],
        # 'eththb': [
        #         f"{rest_url}/market/bids?sym={mapped_currency['eththb']}&lmt=5",
        #         f"{rest_url}/market/asks?sym={mapped_currency['eththb']}&lmt=5"],
        # 'dogethb': [
        #         f"{rest_url}/market/bids?sym={mapped_currency['dogethb']}&lmt=5",
        #         f"{rest_url}/market/asks?sym={mapped_currency['dogethb']}&lmt=5"],
        # 'manathb': [
        #         f"{rest_url}/market/bids?sym={mapped_currency['manathb']}&lmt=5",
        #         f"{rest_url}/market/asks?sym={mapped_currency['manathb']}&lmt=5"]
    }

    while True:

        start = time.time()
        response_dict = {}
        for k in url_dict.keys():
            """saving to postgres has enough pace to put it here, and make loop :
            get() --> save --> next get() --> save() without loosing coherence of data"""

            response_dict[k] = [ast.literal_eval(s.get(url_dict[k][0]).text), ast.literal_eval(s.get(url_dict[k][1]).text)]
            print(time.time()-start, response_dict[k])




asyncio.run(main())