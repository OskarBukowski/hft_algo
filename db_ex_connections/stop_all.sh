#!/bin/bash


DIRS=( $(ls -l | grep "^d" | awk '{print $9}') )
PYTHON_PROCESSES=$(ps -aux | grep ".py$" | awk '{print $12}')

echo "Python processes to check:" $PYTHON_PROCESSES

for process in $PYTHON_PROCESSES; do
	IFS='_'
	read -ra NAME <<< "$process"
	if [[ "${DIRS[*]}" =~ ${NAME[0]} ]]; then
		unset IFS
		echo "Killing process:" $process
		./stop.sh $process
		echo "$(whoami) started $process on $(date "+%D %T %n")" >> history.txt
	fi
done
