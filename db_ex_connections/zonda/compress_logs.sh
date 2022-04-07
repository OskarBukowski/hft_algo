#!/bin/bash

# The input from the is be the name of exchange

now=$(date +"%D")


./stop.sh  "$1_ob.py"
./stop.sh "$1_trades.py"


echo "Compressing $1 logs from ${now}"
file="$1_${now}.tar.gz"
echo $file
tar -czf "log/$file" "$1.log"


./start.sh "$1_ob.py"
./start.sh "$1_trades.py"
