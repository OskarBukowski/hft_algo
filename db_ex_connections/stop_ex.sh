#!/bin/bash


PATH_TO_EX=$(pwd)

for arg in "$@"; do
  python3 "$PATH_TO_EX"/db_ex_connections/start_stop_all.py --stop_ex "$arg"
done
