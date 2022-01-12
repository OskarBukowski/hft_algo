#####
# This file show simplest on-demand request using REST
# It returns up to 300 buy and sell offer on current moment
#####


import requests

url = "https://api.zonda.exchange/rest/trading/orderbook/BTC-PLN"

headers = {'content-type': 'application/json'}

response = requests.request("GET", url, headers=headers)

print(response.text)