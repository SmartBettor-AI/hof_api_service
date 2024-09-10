#!/bin/bash

# Function to run a Python process on specified CPU cores
run_python_process() {
    local cpu_cores=$1
    local script=$2
    local log_file=$3
    # Using absolute path and ensuring stdout and stderr are both redirected to the log file
    nohup python3 $script > $log_file 2>&1 &
}

# Use absolute paths to avoid relative path issues
script_path="/home/ec2-user/hof/HOF-Website/functionality/scraper.py"
log_file="/home/ec2-user/hof/HOF-Website/functionality/bash_scripts/scraper.log"

# Run the Python process (omit taskset if unnecessary)
run_python_process "0,1" "$script_path" "$log_file"

echo "EV runners processes started."