#!/bin/bash

pkill -f functionality/pos_ev_runners/nba_pos_ev_runner.py
pkill -f functionality/pos_ev_runners/nhl_pos_ev_runner.py
pkill -f functionality/pos_ev_runners/mlb_pos_ev_runner.py
pkill -f functionality/pos_ev_runners/wnba_pos_ev_runner.py
pkill -f functionality/pos_ev_runners/pll_pos_ev_runner.py
pkill -f functionality/pos_ev_runners/acon_pos_ev_runner.py
pkill -f functionality/pos_ev_runners/copa_pos_ev_runner.py
pkill -f functionality/pos_ev_runners/epl_pos_ev_runner.py
pkill -f functionality/pos_ev_runners/bunde_pos_ev_runner.py
pkill -f functionality/pos_ev_runners/seriea_pos_ev_runner.py
pkill -f functionality/pos_ev_runners/laliga_pos_ev_runner.py
pkill -f functionality/pos_ev_runners/uefachamps_pos_ev_runner.py
pkill -f functionality/pos_ev_runners/uefachampsq_pos_ev_runner.py
pkill -f functionality/pos_ev_runners/uefaeuropa_pos_ev_runner.py
pkill -f functionality/pos_ev_runners/mls_pos_ev_runner.py
pkill -f functionality/pos_ev_runners/euros_pos_ev_runner.py
pkill -f functionality/pos_ev_runners/ausopenmens1_pos_ev_runner.py
pkill -f functionality/pos_ev_runners/usopenmens1_pos_ev_runner.py
pkill -f functionality/pos_ev_runners/wimbledonmens1_pos_ev_runner.py
pkill -f functionality/pos_ev_runners/usopenwomens1_pos_ev_runner.py
pkill -f functionality/pos_ev_runners/frenchopenmens1_pos_ev_runner.py
pkill -f functionality/pos_ev_runners/wimbledonwomens1_pos_ev_runner.py
pkill -f functionality/pos_ev_runners/mma_mixed_martial_arts_pos_ev_runner.py
pkill -f functionality/pos_ev_runners/americanfootball_nfl_pos_ev_runner.py
pkill -f functionality/pos_ev_runners/americanfootball_ncaaf_pos_ev_runner.py

pkill -f functionality/pos_ev_runners/master_cacher.py
pkill -f functionality/pos_ev_runners/sport_cacher.py
pkill -f functionality/pos_ev_runners/arb_cacher.py
pkill -f functionality/pos_ev_copier.py

echo "Processes related to ev runners and caching system scripts killed."