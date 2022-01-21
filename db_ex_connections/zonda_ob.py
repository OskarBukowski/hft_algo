#####
# This file show request using REST API
# It returns up to 10 buy and sell offer on current moment
# It saves 5 lines of orderbook
#####

### SCRIPT WORKS --> SAVES TO POSTGRES --> SAVES TO LOGFILE

##
# TO DO :
# 1. Check the status of the answer using hash map
# {"status": "Fail"}  ;  {"status": "Ok"}


import aiohttp
import asyncio
from admin_tools.admin_tools import connection, logger_conf
import time
import json


async def single_url_getter(session, url):
    async with session.get(url) as response:
        message = await response.json()
        return message


async def main():
    cursor = connection()
    logger = logger_conf()
    async with aiohttp.ClientSession() as session:

        exchange_spec_dict = json.load(open('../admin_tools/exchanges'))
        mapped_currency = exchange_spec_dict['currency_mapping'][0]['symbols']
        rest_url = exchange_spec_dict['source'][0]['rest_url']

        url_dict = {'btcpln': f"{rest_url}trading/orderbook-limited/{mapped_currency['btcpln']}/10",
                    'ethpln': f"{rest_url}trading/orderbook-limited/{mapped_currency['ethpln']}/10",
                    'lunapln': f"{rest_url}trading/orderbook-limited/{mapped_currency['lunapln']}/10",
                    'ftmpln': f"{rest_url}trading/orderbook-limited/{mapped_currency['ftmpln']}/10"}

        while True:
            try:
                tasks = []
                for k in url_dict.keys():
                    st = time.time()
                    tasks.append(asyncio.create_task(single_url_getter(session, url_dict[k])))

                responses = await asyncio.gather(*tasks)
                print(responses)
                await asyncio.sleep(5 - (time.time() - st))
                before_db_save = time.time()
                cursor.execute(f"""INSERT INTO zonda.btcpln_ob (ask_0)
                                                VALUES ({float(responses[0]['sell'][0]['ra'])});""")

                logger.debug(f"Time of saving ob for {k}: {time.time() - before_db_save}")
                logger.info(f"Ob received and successfully saved into database for {[*url_dict]} ")


            except (KeyError, RuntimeError) as rest_error:
                print(rest_error)
                logger.error(f" $$ {rest_error} $$ ", exc_info=True)


asyncio.run(main())

# '{str(responses[0]['sell'][1]['ra'])}',
# '{str(responses[0]['sell'][2]['ra'])}',
# '{str(responses[0]['sell'][3]['ra'])}',
# '{str(responses[0]['sell'][4]['ra'])}',
# '{str(responses[0]['sell'][0]['ca'])}',
# '{str(responses[0]['sell'][1]['ca'])}',
# '{str(responses[0]['sell'][2]['ca'])}',
# '{str(responses[0]['sell'][3]['ca'])}',
# '{str(responses[0]['sell'][4]['ca'])}',
# '{str(responses[0]['buy'][0]['ra'])}',
# '{str(responses[0]['buy'][1]['ra'])}',
# '{str(responses[0]['buy'][2]['ra'])}',
# '{str(responses[0]['buy'][3]['ra'])}',
# '{str(responses[0]['buy'][4]['ra'])}',
# '{str(responses[0]['buy'][0]['ca'])}',
# '{str(responses[0]['buy'][1]['ca'])}',
# '{str(responses[0]['buy'][2]['ca'])}',
# '{str(responses[0]['buy'][3]['ca'])}',
# '{str(responses[0]['buy'][4]['ca'])}',
# {int(responses[0]['timestamp'])}