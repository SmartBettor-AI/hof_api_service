#!/bin/bash

# Function to run a Python process on specified CPU cores
run_python_process() {
    local cpu_cores=$1
    local script=$2
    local log_file=$3
    taskset -c $cpu_cores nohup python3 $script > $log_file &
}

run_python_process "0,1" functionality/pos_ev_runners/ev_setup.py ev_setup.log
run_python_process "0,1" functionality/pos_ev_runners/arb_setup.py arb_setup.log
run_python_process "0,1" functionality/pos_ev_runners/market_view_setup.py market_view_setup.log
# Run Python processes on CPU cores 0 and 1
# run_python_process "0,1" functionality/pos_ev_runners/nba_pos_ev_runner.py nba_pos_ev.log
# run_python_process "0,1" functionality/pos_ev_runners/nhl_pos_ev_runner.py nhl_pos_ev.log
run_python_process "0,1" functionality/pos_ev_runners/mlb_pos_ev_runner.py mlb_pos_ev.log
run_python_process "0,1" functionality/pos_ev_runners/wnba_pos_ev_runner.py wnba_pos_ev.log
# run_python_process "0,1" functionality/pos_ev_runners/pll_pos_ev_runner.py mlb_pos_ev.log
# run_python_process "0,1" functionality/pos_ev_runners/acon_pos_ev_runner.py mlb_pos_ev.log
# run_python_process "0,1" functionality/pos_ev_runners/copa_pos_ev_runner.py mlb_pos_ev.log
run_python_process "0,1" functionality/pos_ev_runners/epl_pos_ev_runner.py epl_pos_ev.log
# run_python_process "0,1" functionality/pos_ev_runners/bunde_pos_ev_runner.py mlb_pos_ev.log
# run_python_process "0,1" functionality/pos_ev_runners/seriea_pos_ev_runner.py mlb_pos_ev.log
# run_python_process "0,1" functionality/pos_ev_runners/laliga_pos_ev_runner.py mlb_pos_ev.log
# run_python_process "0,1" functionality/pos_ev_runners/uefachamps_pos_ev_runner.py mlb_pos_ev.log
# run_python_process "0,1" functionality/pos_ev_runners/uefachampsq_pos_ev_runner.py mlb_pos_ev.log
# run_python_process "0,1" functionality/pos_ev_runners/uefaeuropa_pos_ev_runner.py mlb_pos_ev.log
run_python_process "0,1" functionality/pos_ev_runners/mls_pos_ev_runner.py mls_pos_ev.log
# run_python_process "0,1" functionality/pos_ev_runners/euros_pos_ev_runner.py mlb_pos_ev.log
# run_python_process "0,1" functionality/pos_ev_runners/ausopenmens1_pos_ev_runner.py mlb_pos_ev.log
run_python_process "0,1" functionality/pos_ev_runners/usopenmens1_pos_ev_runner.py mlb_pos_ev.log
# run_python_process "0,1" functionality/pos_ev_runners/wimbledonmens1_pos_ev_runner.py mlb_pos_ev.log
run_python_process "0,1" functionality/pos_ev_runners/usopenwomens1_pos_ev_runner.py mlb_pos_ev.log
# run_python_process "0,1" functionality/pos_ev_runners/frenchopenmens1_pos_ev_runner.py mlb_pos_ev.log
# run_python_process "0,1" functionality/pos_ev_runners/wimbledonwomens1_pos_ev_runner.py mlb_pos_ev.log
run_python_process "0,1" functionality/pos_ev_runners/mma_mixed_martial_arts_pos_ev_runner.py mma_mixed_martial_arts.log
run_python_process "0,1" functionality/pos_ev_runners/americanfootball_ncaaf_pos_ev_runner.py americanfootball_ncaaf.log
# run_python_process "0,1" functionality/pos_ev_runners/americanfootball_nfl_pos_ev_runner.py americanfootball_nfl.log
run_python_process "0,1" functionality/pos_ev_runners/master_cacher.py master_cacher.log
run_python_process "0,1" functionality/pos_ev_runners/sport_cacher.py sport_cacher.log

run_python_process "0,1" functionality/pos_ev_runners/arb_cacher.py arb_cacher.log
run_python_process "0,1" functionality/pos_ev_copier.py pos_ev_copier.log

echo "EV runners processes started."
