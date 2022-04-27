#!/usr/bin/env python3

#######
# The purpose of this script is to keep all administrative elements in one file
#######

import sys
sys.path.append("/home/obukowski/Desktop/hft_algo")

import psycopg2
from pathlib import Path
from dotenv import load_dotenv
import logging
import os

env_path = Path('C:/Users/oskar/Desktop/hft_algo/admin') / '.env'
load_dotenv(dotenv_path=env_path)


# def connection():
#     conn = psycopg2.connect(
#         host=os.environ['HOST'],
#         database=os.environ['DATABASE'],
#         user=os.environ['USER'],
#         password=os.environ['PASSWORD'],
#     )
#     conn.autocommit = True
#     cursor = conn.cursor()
#
#     return cursor


def connection():
    conn = psycopg2.connect(
        host='127.0.0.1',
        database='***',
        user='***',
        password='***',
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

    def drop_table(self, exchange_name: str, market: str, data_type: str):
        cursor = self.CONN
        cursor.execute(f'''DROP TABLE {exchange_name}.{market}_{data_type}''')


if __name__ == '__main__':
    cs = SqlConst()

    cs.table_creator_2('bitfinex', 'btcusd', 'trades')
    cs.table_creator_2('bitfinex', 'ethusd', 'trades')
    cs.table_creator_2('bitfinex', 'shibusd', 'trades')
    cs.table_creator_2('bitfinex', 'avaxusd', 'trades')
    cs.table_creator_2('bitfinex', 'filusd', 'trades')
    cs.table_creator_2('bitfinex', 'adausd', 'trades')
    cs.table_creator_2('bitfinex', 'solusd', 'trades')
    cs.table_creator_2('bitfinex', 'xrpusd', 'trades')
    cs.table_creator_2('bitfinex', 'linkusd', 'trades')
    cs.table_creator_2('bitfinex', 'ltcusd', 'trades')
    cs.table_creator_2('bitfinex', 'dotusd', 'trades')
    cs.table_creator_2('bitfinex', 'lunausdt', 'trades')
    cs.table_creator_2('bitfinex', 'uniusd', 'trades')
    cs.table_creator_2('bitfinex', 'dogeusd', 'trades')
    cs.table_creator_2('bitfinex', 'dogeusdt', 'trades')
    cs.table_creator_2('bitfinex', 'maticusd', 'trades')

    # cs.table_creator('bitfinex', 'btcusd', 'ob')
    # cs.table_creator('bitfinex', 'ethusd', 'ob')
    # cs.table_creator('bitfinex', 'shibusd', 'ob')
    # cs.table_creator('bitfinex', 'avaxusd', 'ob')
    # cs.table_creator('bitfinex', 'filusd', 'ob')
    # cs.table_creator('bitfinex', 'adausd', 'ob')
    # cs.table_creator('bitfinex', 'solusd', 'ob')
    # cs.table_creator('bitfinex', 'xrpusd', 'ob')
    # cs.table_creator('bitfinex', 'linkusd', 'ob')
    # cs.table_creator('bitfinex', 'ltcusd', 'ob')
    # cs.table_creator('bitfinex', 'dotusd', 'ob')
    # cs.table_creator('bitfinex', 'lunausdt', 'ob')
    # cs.table_creator('bitfinex', 'uniusd', 'ob')
    # cs.table_creator('bitfinex', 'dogeusd', 'ob')
    # cs.table_creator('bitfinex', 'dogeusdt', 'ob')
    # cs.table_creator('bitfinex', 'maticusd', 'ob')




