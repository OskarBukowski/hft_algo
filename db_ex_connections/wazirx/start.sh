#!/bin/bash

EX="python3"

echo "Starting process: $1"
$EX $1 &

echo "$(whoami) started $1 on $(date "+%D %T %n")" >> history.txt
