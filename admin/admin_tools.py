#######
# The purpose of this script is to keep all administrative elements in one file
#######


import psycopg2
from pathlib import Path
from dotenv import load_dotenv
import os
import logging

env_path = Path('C:/Users/oskar/Desktop/hft_algo/hft_algo/admin') / '.env'
load_dotenv(dotenv_path=env_path)



def connection():
    conn = psycopg2.connect(
        host=os.environ['HOST'],
        database=os.environ['DATABASE'],
        user=os.environ['USER'],
        password=os.environ['PASSWORD'],
    )
    conn.autocommit = True
    cursor = conn.cursor()

    return cursor



def dict_values_getter(d):
    if isinstance(d, dict):
        for v in d.values():
            yield from dict_values_getter(v)
    elif isinstance(d, list):
        for v in d:
            yield from dict_values_getter(v)
    else:
        yield d


def logger_conf(logfile_name):
    logging.basicConfig(filename=logfile_name,
                        format="%(asctime)s.%(msecs)03d %(levelname)s  %(message)s",
                        datefmt='%H:%M:%S'
                        )
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    return logger

# def coins_mapping(exchange_name: str, dictionary: dict, url: str):
#     for k, v in dictionary.items():




class SqlConst:
    """This class should be used to create the standardized tables for new markets"""
    CONN = connection()

    def table_creator(self, exchange_name: str, market: str, data_type: str):
        cursor = self.CONN
        cursor.execute(f'''CREATE TABLE {exchange_name}.{market}_{data_type} (
                                ask_0 float8 NULL,
                                ask_vol_0 float8 NULL,
                                ask_1 float8 NULL,
                                ask_vol_1 float8 NULL,
                                ask_2 float8 NULL,
                                ask_vol_2 float8 NULL,
                                ask_3 float8 NULL,
                                ask_vol_3 float8 NULL,
                                ask_4 float8 NULL,
                                ask_vol_4 float8 NULL,
                                ask_5 float8 NULL,
                                ask_vol_5 float8 NULL,
                                ask_6 float8 NULL,
                                ask_vol_6 float8 NULL,
                                ask_7 float8 NULL,
                                ask_vol_7 float8 NULL,
                                ask_8 float8 NULL,
                                ask_vol_8 float8 NULL,
                                ask_9 float8 NULL,
                                ask_vol_9 float8 NULL,
                                bid_0 float8 NULL,
                                bid_vol_0 float8 NULL,
                                bid_1 float8 NULL,
                                bid_vol_1 float8 NULL,
                                bid_2 float8 NULL,
                                bid_vol_2 float8 NULL,
                                bid_3 float8 NULL,
                                bid_vol_3 float8 NULL,
                                bid_4 float8 NULL,
                                bid_vol_4 float8 NULL,
                                bid_5 float8 NULL,
                                bid_vol_5 float8 NULL,
                                bid_6 float8 NULL,
                                bid_vol_6 float8 NULL,
                                bid_7 float8 NULL,
                                bid_vol_7 float8 NULL,
                                bid_8 float8 NULL,
                                bid_vol_8 float8 NULL,
                                bid_9 float8 NULL,
                                bid_vol_9 float8 NULL,
                                "timestamp" int8 NULL
                        )'''
                       )


    def table_creator_2(self, exchange_name: str, market: str, data_type: str):
        cursor = self.CONN
        cursor.execute(f'''CREATE TABLE {exchange_name}.{market}_{data_type} (
                            id varchar NULL,
                            price float8 NULL,
                            volume float8 NULL,
                            "timestamp" int8 NULL
                        )'''
                       )

    def drop_table(self, exchange_name: str, market: str, data_type: str ):
        cursor = self.CONN
        cursor.execute(f'''DROP TABLE {exchange_name}.{market}_{data_type}''')



if __name__ == '__main__':
        cs = SqlConst()

        cs.table_creator_2('wazirx', 'btcinr', 'trades')
        cs.table_creator_2('wazirx', 'ethinr', 'trades')
        cs.table_creator_2('wazirx', 'dogeinr', 'trades')
        cs.table_creator_2('wazirx', 'maticinr', 'trades')
        cs.table_creator_2('wazirx', 'adainr', 'trades')
        cs.table_creator_2('wazirx', 'ftminr', 'trades')
        cs.table_creator_2('wazirx', 'xrpinr', 'trades')
        cs.table_creator_2('wazirx', 'sandinr', 'trades')
        cs.table_creator_2('wazirx', 'usdtinr', 'trades')
        cs.table_creator_2('wazirx', 'solinr', 'trades')
        cs.table_creator_2('wazirx', 'manainr', 'trades')
        cs.table_creator_2('wazirx', 'dotinr', 'trades')
        cs.table_creator_2('wazirx', 'lunainr', 'trades')
        cs.table_creator_2('wazirx', 'trxinr', 'trades')
        cs.table_creator_2('wazirx', 'vetinr', 'trades')
        cs.table_creator_2('wazirx', 'lunausdt', 'trades')
        cs.table_creator_2('wazirx', 'ethusdt', 'trades')

        cs.table_creator('wazirx', 'btcinr', 'ob')
        cs.table_creator('wazirx', 'ethinr', 'ob')
        cs.table_creator('wazirx', 'dogeinr', 'ob')
        cs.table_creator('wazirx', 'maticinr', 'ob')
        cs.table_creator('wazirx', 'adainr', 'ob')
        cs.table_creator('wazirx', 'ftminr', 'ob')
        cs.table_creator('wazirx', 'xrpinr', 'ob')
        cs.table_creator('wazirx', 'sandinr', 'ob')
        cs.table_creator('wazirx', 'usdtinr', 'ob')
        cs.table_creator('wazirx', 'solinr', 'ob')
        cs.table_creator('wazirx', 'manainr', 'ob')
        cs.table_creator('wazirx', 'dotinr', 'ob')
        cs.table_creator('wazirx', 'lunainr', 'ob')
        cs.table_creator('wazirx', 'trxinr', 'ob')
        cs.table_creator('wazirx', 'vetinr', 'ob')
        cs.table_creator('wazirx', 'lunausdt', 'ob')
        cs.table_creator('wazirx', 'ethusdt', 'ob')

