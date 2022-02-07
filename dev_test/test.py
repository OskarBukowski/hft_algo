######
# In this script I have the proxy response with up to 300 lines of orderbook
# I need to create the async loop that will receive this data every few miliseconds
#####


import json
import psycopg2


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



def remote_connection():
    conn = psycopg2.connect(
        host='192.168.0.52',
        database='postgres',
        user='postgres',
        password='remiksow',
    )
    conn.autocommit = True
    cursor = conn.cursor()

    return cursor


r = remote_connection()

print(r.execute("SELECT * from zonda.btcpln_trades"))
