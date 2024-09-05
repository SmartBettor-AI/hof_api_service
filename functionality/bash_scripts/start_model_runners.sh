#!/bin/bash

# Function to run a Python process on specified CPU cores
run_python_process() {
    local cpu_cores=$1
    local script=$2
    local log_file=$3
    taskset -c $cpu_cores nohup python3 $script > $log_file &
}

# Run Python processes on CPU core 3 (4th core)
run_python_process "3" functionality/model_runners/ai_ev_cache_setup.py logs/ai_ev_cache_setup.out

run_python_process "3" functionality/model_runners/ai_ev_mlb_model.py logs/ai_ev_mlb_model.out
run_python_process "3" functionality/model_runners/ai_ev_ncaaf_model.py logs/ai_ev_ncaaf_model.out
run_python_process "3" functionality/model_runners/ai_ev_nfl_model.py logs/ai_ev_nfl_model.out

run_python_process "3" functionality/model_runners/ai_ev_model_cacher.py logs/ai_ev_model_cacher.out

echo ""
echo "Model runners processes started on CPU core 3."
echo ""