#!/bin/bash


cd /home/obukowski/Desktop/hft_algo/db_ex_connections

dirs=$(ls -d */)

for dir in $dirs; do
  cdir=(${dir////})
  cd "$cdir"
  for pfile in $(find . | grep .py); do
    echo "Starting $pfile"
    bash start.sh "$pfile"
    done
  cd ..
done
