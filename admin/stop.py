#!/usr/bin/env python3

import json
import subprocess
import os

exchange_spec_dict = json.load(open('../admin/exchanges'))
ex = exchange_spec_dict['currency_mapping'].keys()


print(os.listdir("C://Users//oskar//Desktop//hft_algo//db_ex_connections//bitso"))