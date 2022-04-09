#!/bin/bash


DIRS=$(ls -l | grep "^d" | awk '{print $9}')


for dir in $DIRS; do
  echo "Starting process: ${dir}_ob.py"
  sh "${dir}"/start.sh "${dir}_ob.py"
  echo "$(whoami) started ${dir}_ob.py on $(date "+%D %T %n")" >> history.txt
done
