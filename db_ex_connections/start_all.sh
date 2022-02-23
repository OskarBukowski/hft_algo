#!/bin/bash


DIRS=( $(ls -l | grep "^d" | awk '{print $9}') )


for dir in $DIRS; do
  echo "Starting process: ${dir}_ob.py and ${dir}_trades.py"
  sh "$dir"/start.sh "${dir}_ob.py" "${dir}_trades.py"
  echo "$(whoami) started ${dir}_ob.py on $(date "+%D %T %n")" >> history.txt
  echo "$(whoami) started ${dir}_trades.py on $(date "+%D %T %n")" >> history.txt
done
