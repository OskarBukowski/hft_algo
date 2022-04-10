#!/bin/bash


EX="python3"
PROCESS_PID=$(ps -aux | grep -i $1 | grep -i $EX | awk '{print $2}')
echo "Killing process with PID: $PROCESS_PID"
kill $PROCESS_PID
 
echo "$(whoami) stoppped $1 on $(date "+%D %T %n")" >> history.txt
