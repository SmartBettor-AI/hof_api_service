#!/bin/bash

# Function to run a Python process on specified CPU cores
run_python_process() {
    local cpu_cores=$1
    local script=$2
    local log_file=$3
    taskset -c $cpu_cores nohup python3 $script > $log_file &
}

run_python_process "0,1" functionality/scraper.py scraper.log

echo "EV runners processes started."
