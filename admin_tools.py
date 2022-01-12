#####
# The purpose of this script is to keep all administrative elements in one file
#####

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
