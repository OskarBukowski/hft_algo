#!/bin/bash

EX="python3"

echo "Starting process: $1"
$EX $1 &

echo "Started $1 $(date)" >> history.txt
