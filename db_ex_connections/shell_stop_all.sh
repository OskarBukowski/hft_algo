#!/bin/bash


cd /home/obukowski/Desktop/hft_algo/db_ex_connections

dirs=$(ls -d */)

for dir in $dirs; do
  cdir=(${dir////})
  echo "Doing $cdir"
  cd "$cdir"
  echo $(pwd)
  for pfile in $(find . | grep .py); do
    echo "Stopping $pfile"
    bash stop.sh "$pfile"
    done
  cd ..
done














