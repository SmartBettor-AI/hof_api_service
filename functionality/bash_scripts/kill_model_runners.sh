#!/bin/bash


pkill -f functionality/model_runners/ai_ev_mlb_model.py
pkill -f functionality/model_runners/ai_ev_ncaaf_model.py
pkill -f functionality/model_runners/ai_ev_nfl_model.py
pkill -f functionality/model_runners/ai_ev_model_cacher.py

echo ""
echo "Processes related to model runners scripts killed."
echo ""
