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



# bid --> buy
# ask --> sell


from requests import Session
import asyncio
import ast
from admin_tools import connection, logger_conf
import time
import json


async def main():
    cursor = connection()
    s = Session()

    exchange_spec_dict = json.load(open('../admin_tools/exchanges'))
    mapped_currency = exchange_spec_dict['currency_mapping'][0]['symbols']
    rest_url = exchange_spec_dict['source'][0]['rest_url']

    url_dict = {'btcpln': f"{rest_url}/orderbook/{mapped_currency['btcpln']}",
                'ethpln': f"{rest_url}/orderbook/{mapped_currency['ethpln']}",
                'lunapln': f"{rest_url}/orderbook/{mapped_currency['lunapln']}",
                'ftmpln': f"{rest_url}/orderbook/{mapped_currency['ftmpln']}"}
    while True:
        try:
            start = time.time()
            headers = {'content-type': 'application/json'}
            response_dict = {}
            for k in url_dict.keys():
                """saving to postgres has enough pace to put it here, and make loop :
                get() --> save --> next get() --> save() without loosing coherence of data"""

                response_dict[k] = ast.literal_eval(s.get(url_dict[k], headers=headers).text)
                time_before_db_save = time.time()
                cursor.execute(
                    f'''INSERT INTO zonda.{k}_ob (ask_0, ask_1, ask_2, ask_3, ask_4, ask_vol_0, ask_vol_1, ask_vol_2,
                                                    ask_vol_3, ask_vol_4, bid_0, bid_1, bid_2, bid_3, bid_4, bid_vol_0, 
                                                    bid_vol_1, bid_vol_2, bid_vol_3,bid_vol_4, "timestamp")
                                                                            VALUES (
                                                                                    '{str(response_dict[k]['sell'][0]['ra'])}',
                                                                                    '{str(response_dict[k]['sell'][1]['ra'])}',
                                                                                    '{str(response_dict[k]['sell'][2]['ra'])}',
                                                                                    '{str(response_dict[k]['sell'][3]['ra'])}',
                                                                                    '{str(response_dict[k]['sell'][4]['ra'])}',
                                                                                    '{str(response_dict[k]['sell'][0]['ca'])}',
                                                                                    '{str(response_dict[k]['sell'][1]['ca'])}',
                                                                                    '{str(response_dict[k]['sell'][2]['ca'])}',
                                                                                    '{str(response_dict[k]['sell'][3]['ca'])}',
                                                                                    '{str(response_dict[k]['sell'][4]['ca'])}',
                                                                                    '{str(response_dict[k]['buy'][0]['ra'])}',
                                                                                    '{str(response_dict[k]['buy'][1]['ra'])}',
                                                                                    '{str(response_dict[k]['buy'][2]['ra'])}',
                                                                                    '{str(response_dict[k]['buy'][3]['ra'])}',
                                                                                    '{str(response_dict[k]['buy'][4]['ra'])}',
                                                                                    '{str(response_dict[k]['buy'][0]['ca'])}',
                                                                                    '{str(response_dict[k]['buy'][1]['ca'])}',
                                                                                    '{str(response_dict[k]['buy'][2]['ca'])}',
                                                                                    '{str(response_dict[k]['buy'][3]['ca'])}',
                                                                                    '{str(response_dict[k]['buy'][4]['ca'])}',
                                                                                    {int(response_dict[k]['timestamp'])}
                                                                            )'''
                )
                time_after_db_save = time.time()
                logger_conf().debug(f"Time of saving ob for {k}: {time_after_db_save - time_before_db_save}")


            logger_conf().info(f"Ob received and saved into database for {[*url_dict]} ")

            time.sleep(5 - (time.time() - start))



        except KeyError as rest_error:
            logger_conf().error(f" $$ {rest_error} $$ ", exc_info=True)


asyncio.get_event_loop().run_until_complete(main())
asyncio.get_event_loop().run_forever()
