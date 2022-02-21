#!/bin/bash


PYTHON_PROCESSES=$(ps -aux | grep ".py$" | awk '{print $12}')


for process in $PYTHON_PROCESSES; do
	IFS='_'
	read -ra NAME <<< "$process"
	if [ "$process" = "$1_ob.py" ] && [ "$process" = "$1_trades.py" ]; then
		unset IFS
		echo "Killing process:" $process
		./stop.sh $process
		echo "$(whoami) started $process on $(date "+%D %T %n")" >> history.txt
	fi
done
