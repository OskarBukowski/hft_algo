#!/bin/bash

for arg in "$@"; do
  python3 /home/obukowski/Desktop/hft_algo/db_ex_connections/start_stop_all.py --start_ex "$arg"
done