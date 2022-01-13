#####
# This file show request using REST API
# It returns up to 300 buy and sell offer on current moment
# The request execution time is up to 280-300 ms
# It take 5 lines of orderbook

#####

### SCRIPT WORKS --> SAVES TO POSTGRES --> SAVES TO LOGFILE

##
# TO DO :
# 1. Check the status of the answer using hash map
# {"status": "Fail"}  ;  {"status": "Ok"}
# 2. Check the option with time.sleep( for example 100 ms ) because
# approximately I will receive 700K lines of data within one day


# bid --> buy
# ask --> sell



from requests import Session
import asyncio
import ast
from admin_tools import connection, logger_conf


async def main():
    cursor = connection()
    s = Session()
    url = "https://api.zonda.exchange/rest/trading/orderbook/BTC-PLN"
    while True:
        try:
            headers = {'content-type': 'application/json'}
            response = s.get(url, headers=headers)
            dict_val = ast.literal_eval(response.text)

            cursor.execute(
                f'''INSERT INTO zonda.zonda_rest_ob (ask_0, ask_1, ask_2, ask_3, ask_4, ask_vol_0, ask_vol_1, ask_vol_2, 
                ask_vol_3, ask_vol_4, bid_0, bid_1, bid_2, bid_3, bid_4, bid_vol_0, bid_vol_1, bid_vol_2, bid_vol_3,
                bid_vol_4, "timestamp")
                                        VALUES (
                                                '{str(dict_val['sell'][0]['ra'])}',
                                                '{str(dict_val['sell'][1]['ra'])}',
                                                '{str(dict_val['sell'][2]['ra'])}',
                                                '{str(dict_val['sell'][3]['ra'])}',
                                                '{str(dict_val['sell'][4]['ra'])}',
                                                '{str(dict_val['sell'][0]['ca'])}',
                                                '{str(dict_val['sell'][1]['ca'])}',
                                                '{str(dict_val['sell'][2]['ca'])}',
                                                '{str(dict_val['sell'][3]['ca'])}',
                                                '{str(dict_val['sell'][4]['ca'])}',
                                                '{str(dict_val['buy'][0]['ra'])}',
                                                '{str(dict_val['buy'][1]['ra'])}',
                                                '{str(dict_val['buy'][2]['ra'])}',
                                                '{str(dict_val['buy'][3]['ra'])}',
                                                '{str(dict_val['buy'][4]['ra'])}',
                                                '{str(dict_val['buy'][0]['ca'])}',
                                                '{str(dict_val['buy'][1]['ca'])}',
                                                '{str(dict_val['buy'][2]['ca'])}',
                                                '{str(dict_val['buy'][3]['ca'])}',
                                                '{str(dict_val['buy'][4]['ca'])}',
                                                {int(dict_val['timestamp'])}
                                        )'''
            )

            print(response.text)
            logger_conf().info(f"Ob received on timestamp: {dict_val['timestamp']}")


        except KeyError as websocket_error:

            logger_conf().error(f" $$ {websocket_error} $$ ", exc_info=True)



asyncio.get_event_loop().run_until_complete(main())
asyncio.get_event_loop().run_forever()
