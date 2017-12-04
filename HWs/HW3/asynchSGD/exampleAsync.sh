#!/bin/bash

rm serverlog-*
rm asynclog-*

source tfdefs.sh
start_cluster startserver.py
# start multiple clients
echo "running main task"
#nohup python exampleAsynchronousUpdate.py --task_index=0 &
nohup python exampleAsynchronousUpdate.py --task_index=0 > asynclog-0.out 2>&1&
sleep 10 # wait for variable to be initialized
echo "running other tasks"
nohup python exampleAsynchronousUpdate.py --task_index=1 > asynclog-1.out 2>&1&
# nohup python exampleAsynchronousUpdate.py --task_index=2 > asynclog-2.out 2>&1&
# nohup python exampleAsynchronousUpdate.py --task_index=3 > asynclog-3.out 2>&1&
# nohup python exampleAsynchronousUpdate.py --task_index=4 > asynclog-4.out 2>&1&
echo "all tasks are funcitonal"
