#!/bin/bash

EX="python3"

echo "Starting processes for exchange: $1"


sh $1/start.sh "$1_ob.py" "$1_trades.py"


