#####
# The purpose of this script is to keep all administrative elements in one file
#####


#####
# TO DO:
# 1. Prepare the connection function for more log files and more instruments
# 2. Create here the table creator to avoid doing it manual

import psycopg2
from pathlib import Path
from dotenv import load_dotenv
import os
import logging

env_path = Path('.') / '.env'
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


def logger_conf():
    logging.basicConfig(filename="logfile.log",
                        format="%(asctime)s.%(msecs)03d %(levelname)s  %(message)s",
                        datefmt='%H:%M:%S'
                        )
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    return logger


class SqlConst:
    """This class should be used to create the standardized tables for new markets"""
    CONN = connection()

    def table_creator(self, exchange_name: str, data_type: str):
        cursor = self.CONN
        cursor.execute(f'''CREATE TABLE {exchange_name}.{exchange_name}_{data_type} (
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
                                "timestamp" int8 NULL
                        )'''
                       )


    def table_creator_2(self, exchange_name: str, data_type: str):
        cursor = self.CONN
        cursor.execute(f'''CREATE TABLE {exchange_name}.{exchange_name}_{data_type} (
                            id varchar NULL,
                            price float8 NULL,
                            volume float8 NULL,
                            "timestamp" int8 NULL
                        )'''
                       )
