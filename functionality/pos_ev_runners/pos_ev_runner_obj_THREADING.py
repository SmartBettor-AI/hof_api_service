import requests
import os
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
import traceback
import json
import flock as flock
import threading
import time
import sys
import os
import redis
import numpy as np
import math
import logging
import random
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
sys.path.append(parent_dir)
redis_client = redis.Redis(host='localhost', port=6379, db=0)
import psutil
process = psutil.Process(os.getpid())


def map_display_data(column_name, df):
     if column_name == 'sport_title':
        sports = {
           'icehockey_nhl': 'Ice Hockey',
           'americanfootball_ncaaf': 'College Football',
           'americanfootball_nfl': 'Pro Football',
           'basketball_nba': 'Pro Basketball',
           'basketball_ncaab': 'College Basketball',
           'basketball_euroleague':'Pro Basketball',
           'baseball_mlb':'Pro Baseball',
            "basketball_wnba": "Women's Pro Basketball",
            "lacrosse_pll": "Pro Lacrosse",
            "soccer_africa_cup_of_nations": "Pro Soccer",
            "soccer_conmebol_copa_america": "Pro Soccer",
            "soccer_epl": "Pro Soccer",
            "soccer_germany_bundesliga": "Pro Soccer",
            "soccer_italy_serie_a": "Pro Soccer",
            "soccer_spain_la_liga": "Pro Soccer",
            "soccer_uefa_champs_league": "Pro Soccer",
            "soccer_uefa_champs_league_qualification": "Pro Soccer",
            "soccer_uefa_europa_league":"Pro Soccer",
            "soccer_usa_mls":"Pro Soccer",
            "soccer_uefa_european_championship": "Pro Soccer",
            "tennis_atp_aus_open_singles": "Men's Tennis",
            "tennis_atp_us_open":  "Men's Tennis",
            "tennis_atp_wimbledon":  "Men's Tennis",
            "tennis_wta_us_open": "Women's Tennis",
            "tennis_atp_french_open":  "Men's Tennis",
            "tennis_wta_wimbledon": "Women's Tennis",
            "mma_mixed_martial_arts": "Mixed Martial Arts"
        }

        leagues = {
           'icehockey_nhl': 'NHL',
           'americanfootball_ncaaf': 'NCAAF',
           'americanfootball_nfl': 'NFL',
           'basketball_nba': 'NBA',
           'basketball_ncaab': 'NCAAB',
           'basketball_euroleague':'Euroleague',
           'baseball_mlb':'MLB',
            "basketball_wnba": "WNBA",
            "lacrosse_pll": "PLL",
            "soccer_africa_cup_of_nations": "ACON",
            "soccer_conmebol_copa_america": "Copa America",
            "soccer_epl": "EPL",
            "soccer_germany_bundesliga": "BUNDESLIGA",
            "soccer_italy_serie_a": "SERIEA",
            "soccer_spain_la_liga": "LALIGA",
            "soccer_uefa_champs_league": "UEFA Champions League",
            "soccer_uefa_champs_league_qualification": "UEFA Champions League Qualifiers",
            "soccer_uefa_europa_league": "UEFA Europa League",
            "soccer_usa_mls": "MLS",
            "soccer_uefa_european_championship": "European Championship",
            "tennis_atp_aus_open_singles": "Australian Open Singles",
            "tennis_atp_us_open": "US Open Singles",
            "tennis_atp_wimbledon": "Wimbledon Singles",
            "tennis_wta_us_open": "US Open Singles",
            "tennis_atp_french_open": "French Open Singles",
            "tennis_wta_wimbledon": "Wimbledon Singles",
            "mma_mixed_martial_arts": "MMA"
        }
        
        try:
         df['sport_title_display'] = df['sport_title'].map(sports)
         df['sport_league_display'] = df['sport_title'].map(leagues)
        
        except:
            df['sport_title_display'] = 0
            df['sport_league_display'] = 0
           
        return df
     
     elif column_name == 'market':
        markets = {
           'h2h': 'Moneyline',
           'spreads': 'Spread',
           'totals': 'Game Total',
           'alternate_spreads': 'Spread',
           'alternate_totals': 'Game Total',
           'team_totals': 'Team Total',
           'alternate_team_totals': 'Team Total',
           'player_points':'Player Points',
           'player_rebounds':'Player Rebounds',
           'player_assists':'Player Assists',
           'player_threes':'Player Threes',
           'player_double_double':'Player Double Double',
           'player_blocks':'Player Blocks',
           'player_steals':'Player Steals',
           'player_turnovers':'Player Turnovers',
           'player_points_rebounds_assists':'Player Points + Rebounds + Assists',
           'player_points_rebounds':'Player Points + Rebounds',
           'player_points_assists':'Player Points + Assists',
           'player_rebounds_assists':'Player Rebounds + Assists',
           'player_pass_tds':"Player Passing TD's",
           'player_pass_yds':"Player Passing Yards",
           'player_pass_completions':"Player Pass Completions",
           'player_pass_attempts':"Player Pass Attempts",
           'player_pass_interceptions':"Player Interceptions",
           'player_pass_longest_completion':"Player Longest Completion",
           'player_rush_yds':"Player Rushing Yards",
           'player_rush_attempts':"Player Rushing Attempts",
           'player_rush_longest':"Player Longest Rush",
           'player_receptions':"Player Receptions",
           'player_reception_yds':"Player Receiving Yards",
           'player_reception_longest':"Player Longest Reception",
           'player_kicking_points':"Player Kicking Points",
           'player_field_goals':"Player Field Goals",
           'player_tackles_assists':"Player Tackles + Assists",
           'player_power_play_points':"Player Power Play Points",
           'player_blocked_shots':"Player Blocked Shots",
           'player_shots_on_goal': "Player Shots on Goal", 
           'player_total_saves': "Player Total Saves", 
           'player_total_saves': "Player Total Saves", 
           'h2h_q1': '1Q Moneyline',
            'h2h_q2': '2Q Moneyline',
            'h2h_q3': '3Q Moneyline',
            'h2h_q4': '4Q Moneyline',
            'h2h_h1': '1H Moneyline',
            'h2h_h2': '2H Moneyline',
            'h2h_p1': '1P Moneyline',
            'h2h_p2': '2P Moneyline',
            'h2h_p3': '3P Moneyline',
            'spreads_q1': '1Q Spread',
            'spreads_q2': '2Q Spread',
            'spreads_q3': '3Q Spread', 
            'spreads_q4': '4Q Spread',
            'spreads_h1': '1H Spread', 
            'spreads_h2': '2H Spread', 
            'spreads_p1': '1P Spread', 
            'spreads_p2': '2P Spread', 
            'spreads_p3': '3P Spread', 
            'totals_q1': '1Q Total', 
            'totals_q2': '2Q Total', 
            'totals_q3': '3Q Total', 
            'totals_q4': '4Q Total', 
            'totals_h1': '1H Total',
            'totals_h2': '2H Total', 
            'totals_p1': '1P Total', 
            'totals_p2': '2P Total', 
            'totals_p3': '3P Total',

            'batter_singles': 'Batter Singles', 
            'pitcher_strikeouts' : 'Pitcher Strikeouts', 
            'pitcher_outs' : 'Pitcher Outs', 
            'player_blocked_shots_alternate' : 'Player Alt Blocked Shots',
            'batter_stolen_bases' : ' Batter Stolen Bases',
            'h2h_1st_7_innings' : '1st 7 Inning Moneyline', 
            'player_assists_alternate' : 'Player Alt Assists', 
            'pitcher_walks' : "Pitcher Walks",
            'player_blocks_alternate' : "Player Alt Blocks", 
            'batter_strikeouts' : "Batter Strikeouts",
            'batter_hits_runs_rbis' : 'Batter Hits + Runs + RBIs',
            'player_rebounds_alternate' : 'Player Alt Rebounds',

            'totals_1st_1_innings' : "1st Inning Total", 
            'totals_1st_3_innings' : '1st 3 Innings Total',
            'totals_1st_5_innings' : '1st 5 Innings Total', 
            'totals_1st_7_innings' : '1st 7 Innings Total', 
            'alternate_totals_1st_1_innings' : '1st 1 Innings Alt Total', 
            'alternate_totals_1st_3_innings' : '1st 3 Innings Alt Total', 
            'alternate_totals_1st_5_innings' : '1st 5 Innings Alt Total', 
            'alternate_totals_1st_7_innings' : '1st 7 Innings Alt Total', 

            'spreads_1st_1_innings' : "1st Inning Spread",
            'spreads_1st_3_innings' : "1st 3 Innings Spread", 
            'spreads_1st_5_innings' : "1st 5 Innings Spread", 
            'spreads_1st_7_innings' : "1st 7 Innings Spread",
            'alternate_spreads_1st_1_innings' : '1st Inning Alt Spread', 
            'alternate_spreads_1st_3_innings' : '1st 3 Innings Alt Spread', 
            'alternate_spreads_1st_5_innings' : '1st 5 Innings Alt Spread',
            'alternate_spreads_1st_7_innings' : '1st 7 Innings Alt Spread',

            'h2h_1st_1_innings' : "1st Inning Moneyline",
            'h2h_1st_3_innings' : "1st 3 Innings Moneyline",
            'h2h_1st_5_innings' : '1st 5 Innings Moneylne', 
            'h2h_1st_7_innings' : '1st 7 Innings Moneylne', 





            'player_rebounds_assists_alternate' : 'Player Alt Rebounds + Assists',
            'batter_rbis' : "Batter RBIs", 
            'batter_doubles' : "Batter Doubles",
            'player_points_alternate' : 'Player Alt Points',
            'batter_hits': 'Batter Hits',
            'batter_total_bases' : "Batter Total Bases",
            'player_total_saves_alternate' : "Player Alt Saves",
            'player_points_rebounds_alternate' : "Player Alt Points + Rebounds", 'player_power_play_points_alternate' : "Player Alt Power Play Points",
            'batter_walks' : "Batter Walks", 
            'pitcher_hits_allowed' : "Pitcher Hits Allowed",
            'player_points_assists_alternate' : "Player Alt Points + Assists", 
            'player_steals_alternate' : "Player Alt Steals", 
            'batter_runs_scored' : "Batter Runs Scored", 
            'player_points_rebounds_assists_alternate' : "Player Alt Rebounds + Assists", 'player_goals_alternate' : "Player Alt Goals", 
            'batter_triples' : "Batter Triples", 
            'pitcher_earned_runs' : "Pitcher Earned Runs", 
            'batter_home_runs' : "Batter Home Runs", 
            'player_shots_on_goal_alternate' : "Player Alt Shots On Goal", 
            'draw_no_bet': "Draw No Bet",
            'player_shots_on_target': "Player Shots On Target",
            'player_shots': "Player Shots",
            'alternate_spreads_corners': "Alternate Spreads Corners",
            'alternate_totals_corners' : "Alternate Total Corners",
            'alternate_spreads_cards' : "Alternate Spreads Cards",
            'alternate_totals_cards' : "Alternate Total Cards:",
            'alternate_team_totals' : "Alternate Team Totals"
        }

        try: 
         df['market_display'] = df['market_key'].map(markets)
        
        except:
          df['market_display'] = 0

        return df
     
     elif column_name == 'wager':
         # Define a function to check for "over" or "under" in a row
         def check_string(row):
               wager_parts = row['wager'].split("_")
               # Moneylines 
               if len(wager_parts) == 1:
                  return f'{row["wager"]}'
               
               elif len(wager_parts) == 2:
                  if row['wager'].split("_")[1][0] != '-':
                     return f'{row["wager"].split("_")[0]} +{row["wager"].split("_")[1]}'
                  else:
                     return f'{row["wager"].split("_")[0]} {row["wager"].split("_")[1]}'
                  
               elif len(wager_parts) == 3:
                  # Player props, team totals
                  if 'over' in row['wager'].split("_")[0].lower() or 'under' in row['wager'].split("_")[0].lower():
                     return f'{row["wager"].split("_")[0]} {row["wager"].split("_")[1]}'
                  if 'over' in row['wager'].split("_")[1].lower() or 'under' in row['wager'].split("_")[1].lower():
                     return f'{row["wager"].split("_")[0]} {row["wager"].split("_")[1]} {row["wager"].split("_")[2]}'
         
         def check_string_other(row):
               wager_parts = row['wagers_other'].split("_")
               
               # Moneylines 
               if len(wager_parts) == 1:
                  return f'{row["wagers_other"]}'
               
               elif len(wager_parts) == 2:
                  if row['wagers_other'].split("_")[1][0] != '-':
                     return f'{row["wagers_other"].split("_")[0]} +{row["wagers_other"].split("_")[1]}'
                  else:
                     return f'{row["wagers_other"].split("_")[0]} {row["wagers_other"].split("_")[1]}'
                  
               elif len(wager_parts) == 3:
                  # Player props, team totals
                  if 'over' in row['wagers_other'].split("_")[0].lower() or 'under' in row['wagers_other'].split("_")[0].lower():
                     return f'{row["wagers_other"].split("_")[0]} {row["wagers_other"].split("_")[1]}'
                  if 'over' in row['wagers_other'].split("_")[1].lower() or 'under' in row['wagers_other'].split("_")[1].lower():
                     return f'{row["wagers_other"].split("_")[0]} {row["wagers_other"].split("_")[1]} {row["wagers_other"].split("_")[2]}'
                  
                  # Team spreads  
         
         try:        
            df['wager_display'] = df.apply(check_string, axis=1)
         except:
            df['wager_display'] = 0
         try:
            df['wager_display_other'] = df.apply(check_string_other, axis=1)
         except:
            df['wager_display_other'] = 0
         return df
      


class PositiveEVDashboardRunner():
  def __init__(self, sport):

    self.API_KEY = os.environ.get("THE_ODDS_API_KEY")

    self.sport = sport

    self.sport_path = {
       "NBA": "pos_ev_data/nba_pos_ev_data.csv",
       "MLB": "pos_ev_data/mlb_pos_ev_data.csv",
       "NHL": "pos_ev_data/nhl_pos_ev_data.csv",
       "NCAAB": "pos_ev_data/ncaab_pos_ev_data.csv",
       "WNBA": "pos_ev_data/wnba_pos_ev_data.csv",
       "PLL": "pos_ev_data/pll_pos_ev_data.csv",
       "ACON": "pos_ev_data/soccer_africa_cup_of_nations_pos_ev_data.csv",
       "COPA": "pos_ev_data/soccer_conmebol_copa_america_pos_ev_data.csv",
       "EPL": "pos_ev_data/soccer_epl_pos_ev_data.csv",
       "BUNDE": "pos_ev_data/soccer_germany_bundesliga_pos_ev_data.csv",
       "SERIEA": "pos_ev_data/soccer_italy_serie_a_pos_ev_data.csv",
       "LALIGA": "pos_ev_data/soccer_spain_la_liga_pos_ev_data.csv",
       "UEFACHAMPS": "pos_ev_data/soccer_uefa_champs_league_pos_ev_data.csv",
       "UEFACHAMPSQ": "pos_ev_data/soccer_uefa_champs_league_qualification_pos_ev_data.csv",
       "UEFAEUROPA": "pos_ev_data/soccer_uefa_europa_league_pos_ev_data.csv",
       "MLS": "pos_ev_data/soccer_usa_mls_pos_ev_data.csv",
       "EUROS": "pos_ev_data/soccer_uefa_european_championship_pos_ev_data.csv",
       "AUSOPENMENS1": "pos_ev_data/tennis_atp_aus_open_singles_pos_ev_data.csv",
       "USOPENMENS1": "pos_ev_data/tennis_atp_us_open_pos_ev_data.csv",
       "WIMBLEDONMENS1": "pos_ev_data/tennis_atp_wimbledon_pos_ev_data.csv",
       "USOPENWOMENS1": "pos_ev_data/tennis_wta_us_open_pos_ev_data.csv",
       "FRENCHOPENMENS1": "pos_ev_data/tennis_atp_french_open_pos_ev_data.csv",
       "WIMBLEDONWOMENS1": "pos_ev_data/tennis_wta_wimbledon_pos_ev_data.csv",
       "MMA": "pos_ev_data/mma_mixed_martial_arts_pos_ev_data.csv",
       "NFL": "pos_ev_data/americanfootball_nfl_pos_ev_data.csv",
       "CFB": "pos_ev_data/americanfootball_ncaaf.csv"

       
       }
    
    self.ev_cache_paths = {
       "NBA": "nba_pos_ev_cache",
       "MLB": "mlb_pos_ev_cache",
       "NHL": "nhl_pos_ev_cache",
       "NCAAB": "ncaab_pos_ev_cache",
       "WNBA": "wnba_pos_ev_cache",
       "PLL": "pll_pos_ev_cache",
       "ACON": "soccer_africa_cup_of_nations_pos_ev_cache",
       "COPA": "soccer_conmebol_copa_america_pos_ev_cache",
       "EPL": "soccer_epl_pos_ev_cache",  
       "BUNDE": "soccer_germany_bundesliga_pos_ev_cache",
       "SERIEA": "soccer_italy_serie_a_pos_ev_cache",
       "LALIGA": "soccer_spain_la_liga_pos_ev_cache",
       "UEFACHAMPS": "soccer_uefa_champs_league_pos_ev_cache",
       "UEFACHAMPSQ": "soccer_uefa_champs_league_qualification_pos_ev_cache",
       "UEFAEUROPA": "soccer_uefa_europa_league_pos_ev_cache",
       "MLS": "soccer_usa_mls_pos_ev_cache",
       "EUROS": "soccer_uefa_european_championship_pos_ev_cache",
       "AUSOPENMENS1": "tennis_atp_aus_open_singles_pos_ev_cache",
       "USOPENMENS1": "tennis_atp_us_open_pos_ev_cache",
       "WIMBLEDONMENS1": "tennis_atp_wimbledon_pos_ev_cache",
       "USOPENWOMENS1": "tennis_wta_us_open_pos_ev_cache",
       "FRENCHOPENMENS1": "tennis_atp_french_open_pos_ev_cache",
       "WIMBLEDONWOMENS1": "tennis_wta_wimbledon_pos_ev_cache",
       "MMA": "mma_mixed_martial_arts_pos_ev_cache",
       "NFL": "americanfootball_nfl_pos_ev_cache",
       "CFB": "americanfootball_ncaaf_pos_ev_cache"
   
    }

    self.arb_cache_paths = {
       "NBA": "nba_arb_cache",
       "MLB": "mlb_arb_cache",
       "NHL": "nhl_arb_cache",
       "NCAAB": "ncaab_arb_cache",
       "WNBA": "wnba_arb_cache",
       "PLL": "pll_arb_cache",
       "ACON": "soccer_africa_cup_of_nations_arb_cache",
       "COPA": "soccer_africa_cup_of_nations_arb_cache",
       "EPL": "soccer_epl_arb_cache",
       "BUNDE": "soccer_germany_bundesliga_arb_cache",
       "SERIEA": "soccer_italy_serie_a_arb_cache",
       "LALIGA": "soccer_spain_la_liga_arb_cache",
       "UEFACHAMPS": "soccer_uefa_champs_league_arb_cache",
       "UEFACHAMPSQ": "soccer_uefa_champs_league_qualification_arb_cache",
       "UEFAEUROPA": "soccer_uefa_europa_league_arb_cache",
       "MLS": "soccer_usa_mls_arb_cache",
       "EUROS": "soccer_uefa_european_championship_arb_cache",
       "AUSOPENMENS1": "tennis_atp_aus_open_singles_arb_cache",
       "USOPENMENS1": "tennis_atp_us_open_arb_cache",
       "WIMBLEDONMENS1": "tennis_atp_wimbledon_arb_cache",
       "USOPENWOMENS1": "tennis_wta_us_open_arb_cache",
       "FRENCHOPENMENS1": "tennis_atp_french_open_arb_cache",
       "WIMBLEDONWOMENS1": "tennis_wta_wimbledon_arb_cache",
       "MMA": "mma_mixed_martial_arts_arb_cache",
       "NFL": "americanfootball_nfl_arb_cache",
       "CFB": "americanfootball_ncaaf_arb_cache"
    }

    self.market_view_cache_paths = {
       "NBA": "nba_market_view_cache",
       "MLB": "mlb_market_view_cache",
       "NHL": "nhl_market_view_cache",
       "NCAAB": "ncaab_market_view_cache",
       "WNBA": "wnba_market_view_cache",
       "PLL": "pll_market_view_cache",
       "ACON": "soccer_africa_cup_of_nations_market_view_cache",
       "COPA": "soccer_conmebol_copa_america_market_view_cache",
       "EPL": "soccer_epl_market_view_cache",
       "BUNDE": "soccer_germany_bundesliga_market_view_cache",
       "SERIEA": "soccer_italy_serie_a_market_view_cache",
       "LALIGA": "soccer_spain_la_liga_market_view_cache",
       "UEFACHAMPS": "soccer_uefa_champs_league_market_view_cache",
       "UEFACHAMPSQ": "soccer_uefa_champs_league_qualification_market_view_cache",
       "UEFAEUROPA": "soccer_uefa_europa_league_market_view_cache",
       "MLS": "soccer_usa_mls_market_view_cache",
       "EUROS": "soccer_uefa_european_championship_market_view_cache",
       "AUSOPENMENS1": "tennis_atp_aus_open_singles_market_view_cache",
       "USOPENMENS1": "tennis_atp_us_open_market_view_cache",
       "WIMBLEDONMENS1": "tennis_atp_wimbledon_market_view_cache",
       "USOPENWOMENS1": "tennis_wta_us_open_market_view_cache",
       "FRENCHOPENMENS1": "tennis_atp_french_open_market_view_cache",
       "WIMBLEDONWOMENS1": "tennis_wta_wimbledon_market_view_cache",
       "MMA": "mma_mixed_martial_arts_market_view_cache",
       "NFL": "americanfootball_nfl_market_view_cache",
       "CFB": "americanfootball_ncaaf_market_view_cache"

    }

    self.arb_sport_path = {
       "NBA": "arb_data/nba_arb_data.csv",
       "MLB": "arb_data/mlb_arb_data.csv",
       "NHL": "arb_data/nhl_arb_data.csv",
       "NCAAB": "arb_data/ncaab_arb_data.csv",
       "WNBA": "arb_data/wnba_arb_data.csv",
       "PLL": "arb_data/pll_arb_data.csv",
       "ACON": "arb_data/soccer_africa_cup_of_nations_arb_data.csv",
       "COPA": "arb_data/soccer_conmebol_copa_america_arb_data.csv",
       "EPL": "arb_data/soccer_epl_arb_data.csv",
       "BUNDE": "arb_data/soccer_germany_bundesliga_arb_data.csv",
       "SERIEA": "arb_data/soccer_italy_serie_a_arb_data.csv",
       "LALIGA": "arb_data/soccer_spain_la_liga_arb_data.csv",
       "UEFACHAMPS": "arb_data/soccer_uefa_champs_league_arb_data.csv",
       "UEFACHAMPSQ": "arb_data/soccer_uefa_champs_league_qualification_arb_data.csv",
       "UEFAEUROPA": "arb_data/soccer_uefa_europa_league_arb_data.csv",
       "MLS": "arb_data/soccer_usa_mls_arb_data.csv",
       "EUROS": "arb_data/soccer_uefa_european_championship_arb_data.csv",
       "AUSOPENMENS1": "arb_data/tennis_atp_aus_open_singles_arb_data.csv",
       "USOPENMENS1": "arb_data/tennis_atp_us_open_arb_data.csv",
       "WIMBLEDONMENS1": "arb_data/tennis_atp_wimbledon_arb_data.csv",
       "USOPENWOMENS1": "arb_data/tennis_wta_us_open_arb_data.csv",
       "FRENCHOPENMENS1": "arb_data/tennis_atp_french_open_arb_data.csv",
       "WIMBLEDONWOMENS1": "arb_data/tennis_wta_wimbledon_arb_data.csv",
       "MMA": "arb_data/mma_mixed_martial_arts_arb_data.csv",
       "NFL": "arb_data/americanfootball_nfl_arb_data.csv",
       "CFB": "arb_data/americanfootball_ncaaf_arb_data.csv"
       }
    
    self.market_view_sport_path = {
       "NBA": "market_view_data/nba_market_view_data.csv",
       "MLB": "market_view_data/mlb_market_view_data.csv",
       "NHL": "market_view_data/nhl_market_view_data.csv",
       "NCAAB": "market_view_data/ncaab_market_view_data.csv",
       "WNBA": "market_view_data/wnba_market_view_data.csv",
       "PLL": "market_view_data/pll_market_view_data.csv",
       "ACON": "market_view_data/soccer_africa_cup_of_nations_market_view_data.csv",
       "COPA": "market_view_data/soccer_conmebol_copa_america.csv",
       "EPL": "market_view_data/soccer_epl_market_view_data.csv",
       "BUNDE": "market_view_data/soccer_germany_bundesliga_market_view_data.csv",
       "SERIEA": "market_view_data/soccer_italy_serie_a_market_view_data.csv",
       "LALIGA": "market_view_data/soccer_spain_la_liga_market_view_data.csv",
       "UEFACHAMPS": "market_view_data/soccer_uefa_champs_league_market_view_data.csv",
       "UEFACHAMPSQ": "market_view_data/soccer_uefa_champs_league_qualification_market_view_data.csv",
       "UEFAEUROPA": "market_view_data/soccer_uefa_europa_league_market_view_data.csv",
       "MLS": "market_view_data/soccer_usa_mls_market_view_data.csv",
       "EUROS": "market_view_data/soccer_uefa_european_championship_market_view_data.csv",
       "AUSOPENMENS1": "market_view_data/tennis_atp_aus_open_singles_market_view_data.csv",
       "USOPENMENS1": "market_view_data/tennis_atp_us_open_market_view_data.csv",
       "WIMBLEDONMENS1": "market_view_data/tennis_atp_wimbledon_market_view_data.csv",
       "USOPENWOMENS1": "market_view_data/tennis_wta_us_open_market_view_data.csv",
       "FRENCHOPENMENS1": "market_view_data/tennis_atp_french_open_market_view_data.csv",
       "WIMBLEDONWOMENS1": "market_view_data/tennis_wta_wimbledon_market_view_data.csv",
       "MMA": "market_view_data/mma_mixed_martial_arts_market_view_data.csv",
       "NFL": "market_view_data/americanfootball_nfl_market_view_data.csv",
       "CFB": "market_view_data/americanfootball_ncaaf_market_view_data.csv",
       
      

       }
    
    self.sport_names = {
       "NBA": "basketball_nba",
       "MLB": "baseball_mlb",
       "NHL": "icehockey_nhl",
       "NCAAB": "basketball_ncaab",
       "WNBA": "basketball_wnba",
       "PLL": "lacrosse_pll",
       "ACON": "soccer_africa_cup_of_nations",
       "COPA": "soccer_conmebol_copa_america",
       "EPL": "soccer_epl",
       "BUNDE": "soccer_germany_bundesliga",
       "SERIEA": "soccer_italy_serie_a",
       "LALIGA": "soccer_spain_la_liga",
       "UEFACHAMPS": "soccer_uefa_champs_league",
       "UEFACHAMPSQ": "soccer_uefa_champs_league_qualification",
       "UEFAEUROPA": "soccer_uefa_europa_league",
       "MLS": "soccer_usa_mls",
       "EUROS": "soccer_uefa_european_championship",
       "AUSOPENMENS1": "tennis_atp_aus_open_singles",
       "USOPENMENS1": "tennis_atp_us_open",
       "WIMBLEDONMENS1": "tennis_atp_wimbledon",
       "USOPENWOMENS1": "tennis_wta_us_open",
       "FRENCHOPENMENS1": "tennis_atp_french_open",
       "WIMBLEDONWOMENS1": "tennis_wta_wimbledon",
       "MMA": "mma_mixed_martial_arts",
       "NFL": "americanfootball_nfl",
       "CFB": "americanfootball_ncaaf"
       }

    self.sports = [self.sport_names[self.sport]]

    self.file_output_path = self.sport_path[self.sport]

    self.arb_file_output_path = self.arb_sport_path[self.sport]

    self.market_view_file_output_path = self.market_view_sport_path[self.sport]


    self.markets_sports = {
       'americanfootball_ncaaf': ['h2h', 'spreads', 'totals', 'alternate_spreads', 'alternate_totals','alternate_team_totals', 'team_totals','player_pass_tds', 'player_pass_yds', 'player_pass_completions', 'player_pass_attempts', 'player_pass_interceptions', 'player_pass_longest_completion', 'player_rush_yds', 'player_rush_attempts', 'player_rush_longest', 'player_receptions', 'player_reception_yds', 'player_reception_longest', 'player_kicking_points', 'player_field_goals', 'player_tackles_assists', 'h2h_q1', 'h2h_q2', 'h2h_q3', 'h2h_q4', 'h2h_h1', 'h2h_h2', 'spreads_q1','spreads_q2', 'spreads_q3', 'spreads_q4', 'spreads_h1', 'spreads_h2', 'totals_q1', 'totals_q2', 'totals_q3', 'totals_q4', 'totals_h1', 'totals_h2', 'alternate_spreads_q1','alternate_spreads_h1', 'alternate_totals_q1', 'alternate_totals_h1', 'team_totals_h1', 'team_totals_h2', 'team_totals_q1','team_totals_q2', 'team_totals_q3','team_totals_q4', 'alternate_team_totals_h1', 'alternate_team_totals_h2' ],
       'americanfootball_nfl': ['h2h', 'spreads', 'totals', 'alternate_spreads', 'alternate_totals','alternate_team_totals', 'team_totals','player_pass_tds', 'player_pass_yds', 'player_pass_completions', 'player_pass_attempts', 'player_pass_interceptions', 'player_pass_longest_completion', 'player_rush_yds', 'player_rush_attempts', 'player_rush_longest', 'player_receptions', 'player_reception_yds', 'player_reception_longest', 'player_kicking_points', 'player_field_goals', 'player_tackles_assists', 'player_pass_tds_alternate', 'player_pass_yds_alternate', 'player_rush_yds_alternate', 'player_rush_reception_yds_alternate','player_reception_yds_alternate', 'player_receptions_alternate','h2h_q1', 'h2h_q2', 'h2h_q3', 'h2h_q4', 'h2h_h1', 'h2h_h2', 'spreads_q1','spreads_q2', 'spreads_q3', 'spreads_q4', 'spreads_h1', 'spreads_h2', 'totals_q1', 'totals_q2', 'totals_q3', 'totals_q4', 'totals_h1', 'totals_h2', 'alternate_spreads_q1','alternate_spreads_h1', 'alternate_totals_q1', 'alternate_totals_h1', 'team_totals_h1', 'team_totals_h2', 'team_totals_q1','team_totals_q2', 'team_totals_q3','team_totals_q4', 'alternate_team_totals_h1', 'alternate_team_totals_h2' ],

       
       'basketball_nba': [
          'h2h','spreads', 'totals', 'alternate_spreads', 'alternate_totals','team_totals', 'alternate_team_totals',
          
          'player_points', 'player_rebounds', 'player_assists', 'player_threes', 'player_double_double', 'player_blocks', 'player_steals', 'player_turnovers', 'player_points_rebounds_assists', 'player_points_rebounds', 'player_points_assists', 'player_rebounds_assists', 
          
          'player_points_alternate', 'player_rebounds_alternate', 'player_assists_alternate', 'player_blocks_alternate', 'player_steals_alternate', 'player_steals_alternate', 'player_points_assists_alternate', 'player_points_rebounds_alternate', 'player_rebounds_assists_alternate', 'player_points_rebounds_assists_alternate'
          
          'h2h_q1', 'h2h_q2', 'h2h_q3', 'h2h_q4', 'h2h_h1', 'h2h_h2', 'spreads_q1','spreads_q2', 'spreads_q3', 'spreads_q4', 'spreads_h1', 'spreads_h2', 'totals_q1', 'totals_q2', 'totals_q3', 'totals_q4', 'totals_h1', 'totals_h2'
          ],

       'basketball_ncaab':[
          'h2h', 'spreads', 'totals', 'alternate_spreads', 'alternate_totals', 'alternate_team_totals', 'team_totals', 
          
          'player_points', 'player_rebounds', 'player_assists', 'player_threes', 'player_double_double', 'player_blocks', 'player_steals', 'player_turnovers', 'player_points_rebounds_assists', 'player_points_rebounds', 'player_points_assists', 'player_rebounds_assists', 
          
          'h2h_q1', 'h2h_q2', 'h2h_q3', 'h2h_q4', 'h2h_h1', 'h2h_h2', 'spreads_q1','spreads_q2', 'spreads_q3', 'spreads_q4', 'spreads_h1', 'spreads_h2', 'totals_q1', 'totals_q2', 'totals_q3', 'totals_q4', 'totals_h1', 'totals_h2'
                           ],

       'basketball_euroleague':['h2h', 'spreads', 'totals', 'alternate_spreads', 'alternate_totals', 'team_totals'],

       'icehockey_nhl':[
          
          'h2h', 'spreads', 'totals', 'alternate_spreads', 'alternate_totals', 'team_totals', 'alternate_team_totals',
                        
         'player_points', 'player_power_play_points', 'player_assists', 'player_blocked_shots', 'player_shots_on_goal', 'player_total_saves', 

         'player_points_alternate', 'player_assists_alternate', 'player_power_play_points_alternate', 'player_goals_alternate', 'player_shots_on_goal_alternate', 'player_blocked_shots_alternate', 'player_total_saves_alternate',
         
         'h2h_p1', 'h2h_p2', 'h2h_p3', 'spreads_p1', 'spreads_p2', 'spreads_p3', 'totals_p1', 'totals_p2', 'totals_p3',
         
         ],

       'baseball_mlb': [
          'h2h', 'spreads', 'totals', 'alternate_spreads', 'alternate_totals', 'team_totals', 'alternate_team_totals',

         'batter_home_runs', 'batter_hits', 'batter_total_bases', 'batter_rbis', 'batter_runs_scored', 'batter_hits_runs_rbis', 'batter_singles', 'batter_doubles', 'batter_triples', 'batter_walks', 'batter_strikeouts', 'batter_stolen_bases', 'pitcher_strikeouts', 'pitcher_hits_allowed', 'pitcher_walks', 'pitcher_earned_runs', 'pitcher_outs',

         'h2h_1st_1_innings', 'h2h_1st_3_innings', 'h2h_1st_5_innings', 'h2h_1st_7_innings', 'spreads_1st_1_innings', 'spreads_1st_3_innings', 'spreads_1st_5_innings', 'spreads_1st_7_innings', 'alternate_spreads_1st_1_innings', 
         'alternate_spreads_1st_3_innings','alternate_spreads_1st_5_innings','alternate_spreads_1st_7_innings',

         'totals_1st_1_innings', 'totals_1st_3_innings', 'totals_1st_5_innings', 'totals_1st_7_innings', 'alternate_totals_1st_1_innings','alternate_totals_1st_3_innings','alternate_totals_1st_5_innings','alternate_totals_1st_7_innings'
         
         ],



       'soccer_africa_cup_of_nations': [ 'spreads', 'totals', 'alternate_spreads', 'team_totals', 'draw_no_bet',  'alternate_totals', 'player_shots_on_target', 'player_shots', 'player_assists', 'alternate_spreads_corners', 'alternate_totals_corners', 'alternate_spreads_cards', 'alternate_totals_cards'],
       'soccer_conmebol_copa_america': ['spreads', 'totals', 'alternate_spreads', 'team_totals', 'draw_no_bet',  'alternate_totals', 'player_shots_on_target', 'player_shots', 'player_assists', 'alternate_spreads_corners', 'alternate_totals_corners', 'alternate_spreads_cards', 'alternate_totals_cards'],
       'soccer_epl': [ 'spreads', 'totals', 'alternate_spreads', 'team_totals', 'draw_no_bet',  'alternate_totals', 'player_shots_on_target', 'player_shots', 'player_assists', 'alternate_spreads_corners', 'alternate_totals_corners', 'alternate_spreads_cards', 'alternate_totals_cards'],
       'soccer_germany_bundesliga': [ 'spreads', 'totals', 'alternate_spreads', 'team_totals', 'draw_no_bet',  'alternate_totals', 'player_shots_on_target', 'player_shots', 'player_assists', 'alternate_spreads_corners', 'alternate_totals_corners', 'alternate_spreads_cards', 'alternate_totals_cards'],
       'soccer_italy_serie_a': [ 'spreads', 'totals', 'alternate_spreads', 'team_totals', 'draw_no_bet', 'alternate_totals', 'player_shots_on_target', 'player_shots', 'player_assists', 'alternate_spreads_corners', 'alternate_totals_corners', 'alternate_spreads_cards', 'alternate_totals_cards'],
       'soccer_spain_la_liga': [ 'spreads', 'totals', 'alternate_spreads', 'team_totals', 'draw_no_bet',  'alternate_totals', 'player_shots_on_target', 'player_shots', 'player_assists', 'alternate_spreads_corners', 'alternate_totals_corners', 'alternate_spreads_cards', 'alternate_totals_cards'],
       'soccer_uefa_champs_league': [ 'spreads', 'totals', 'alternate_spreads', 'team_totals', 'draw_no_bet', 'alternate_totals', 'player_shots_on_target', 'player_shots', 'player_assists', 'alternate_spreads_corners', 'alternate_totals_corners', 'alternate_spreads_cards', 'alternate_totals_cards'],
       'soccer_uefa_champs_league_qualification': [ 'spreads', 'totals', 'alternate_spreads', 'team_totals', 'draw_no_bet',  'alternate_totals', 'player_shots_on_target', 'player_shots', 'player_assists', 'alternate_spreads_corners', 'alternate_totals_corners', 'alternate_spreads_cards', 'alternate_totals_cards'],
       'soccer_uefa_europa_league': [ 'spreads', 'totals', 'alternate_spreads', 'team_totals', 'draw_no_bet', 'alternate_totals', 'player_shots_on_target', 'player_shots', 'player_assists', 'alternate_spreads_corners', 'alternate_totals_corners', 'alternate_spreads_cards', 'alternate_totals_cards'],
       'soccer_usa_mls': ['spreads', 'totals', 'alternate_spreads', 'team_totals', 'draw_no_bet', 'alternate_totals', 'player_shots_on_target', 'player_shots', 'player_assists', 'alternate_spreads_corners', 'alternate_totals_corners', 'alternate_spreads_cards', 'alternate_totals_cards'],
       'soccer_uefa_european_championship': [ 'spreads', 'totals', 'alternate_spreads', 'team_totals', 'draw_no_bet', 'alternate_totals', 'player_shots_on_target', 'player_shots', 'player_assists', 'alternate_spreads_corners', 'alternate_totals_corners', 'alternate_spreads_cards', 'alternate_totals_cards'],
       "tennis_atp_aus_open_singles": ['h2h','spreads', 'totals', 'alternate_spreads','alternate_totals'],
       "tennis_atp_us_open": ['h2h','spreads', 'totals', 'alternate_spreads','alternate_totals'],
       "tennis_atp_wimbledon": ['h2h', 'spreads', 'totals', 'alternate_spreads','alternate_totals'],
       "tennis_wta_us_open": ['h2h', 'spreads', 'totals', 'alternate_spreads','alternate_totals'],
       "tennis_atp_french_open": ['h2h', 'spreads', 'totals', 'alternate_spreads','alternate_totals'],
       "tennis_wta_wimbledon": ['h2h', 'spreads', 'totals', 'alternate_spreads','alternate_totals'],

       "lacrosse_pll":['h2h', 'spreads', 'totals', 'alternate_spreads', 'alternate_totals', 'alternate_team_totals', 'team_totals', 
          
          'player_points', 'player_turnovers', 'h2h_q1', 'h2h_q2', 'h2h_q3', 'h2h_q4', 'h2h_h1', 'h2h_h2', 'spreads_q1','spreads_q2', 'spreads_q3', 'spreads_q4', 'spreads_h1', 'spreads_h2', 'totals_q1', 'totals_q2', 'totals_q3', 'totals_q4', 'totals_h1', 'totals_h2'],
       "basketball_wnba":['h2h', 'spreads', 'totals', 'alternate_spreads', 'alternate_totals', 'alternate_team_totals', 'team_totals', 
          
          'player_points', 'player_rebounds', 'player_assists', 'player_threes', 'player_double_double', 'player_blocks', 'player_steals', 'player_turnovers', 'player_points_rebounds_assists', 'player_points_rebounds', 'player_points_assists', 'player_rebounds_assists', 
          
          'h2h_q1', 'h2h_q2', 'h2h_q3', 'h2h_q4', 'h2h_h1', 'h2h_h2', 'spreads_q1','spreads_q2', 'spreads_q3', 'spreads_q4', 'spreads_h1', 'spreads_h2', 'totals_q1', 'totals_q2', 'totals_q3', 'totals_q4', 'totals_h1', 'totals_h2'],
          'mma_mixed_martial_arts': ['h2h', 'spreads', 'totals', 'alternate_totals']


    }

    self.featured_betting_markets = [
       'h2h', 
       'spreads', 
       'totals', 
       'outrights', 
       'h2h_lay', 
       'outrights_lay'
       ]

    self.additional_markets = [
       'alternate_spreads', 
       'alternate_totals', 
       'btts', 
       'draw_no_bet', 
       'team_totals'
       ] #h2h_3_way

    self.game_period_markets = [
       'h2h_q1', 'h2h_q2', 'h2h_q3', 'h2h_q4', 'h2h_h1', 'h2h_h2', 'h2h_p1',
       'h2h_p2', 'h2h_p3', 'spreads_q1', 'spreads_q2', 'spreads_q3', 'spreads_q4',
       'spreads_h1', 'spreads_h2', 'spreads_p1', 'spreads_p2', 'spreads_p3', 'totals_q1', 'totals_q2', 'totals_q3', 'totals_q4', 'totals_h1', 'totals_h2', 'totals_p1', 'totals_p2', 'totals_p3'
       ]

    self.nfl_ncaaf_player_props_markets = [
        'player_pass_tds',
        'player_pass_yds', 
        'player_pass_completions', 
        'player_pass_attempts', 
        'player_pass_interceptions', 
        'player_pass_longest_completion', 
        'player_rush_yds', 
        'player_rush_attempts', 
        'player_rush_longest', 
        'player_receptions', 
        'player_reception_yds', 
        'player_reception_longest', 
        'player_kicking_points', 
        'player_field_goals', 
        'player_tackles_assists', 
        'player_1st_td', 
        'player_last_td', 
        'player_anytime_td'
        ]

    self.nba_ncaab_wnba_player_props_markets = [
       'player_points', 
       'player_rebounds', 
       'player_assists', 
       'player_threes', 
       'player_double_double', 
       'player_blocks', 
       'player_steals', 
       'player_turnovers', 
       'player_points_rebounds_assists', 
       'player_points_rebounds', 
       'player_points_assists', 
       'player_rebounds_assists'
       ]

    self.nhl_player_props_markets = [
       'player_points', 
       'player_power_play_points', 
       'player_assists', 
       'player_blocked_shots', 
       'player_shots_on_goal', 
       'player_total_saves', 
       'player_goal_scorer_first', 
       'player_goal_scorer_last', 
       'player_goal_scorer_anytime'
       ]

    self.afl_player_props_markets=[
       'player_disposals', 
       'player_disposals_over', 
       'player_goal_scorer_first', 
       'player_goal_scorer_last', 
       'player_goal_scorer_anytime', 
       'player_goals_scored_over'
       ]

    self.market_type_dict = {
       'h2h': 'moneyline',
       'spreads': 'brown',
       'totals': 'spreads_and_totals',
       'alternate_totals': 'spreads_and_totals',
       'alternate_spreads': 'brown',
       'team_totals': 'green',
       'player_pass_tds': 'green',
       'player_pass_yds': 'green',
       'player_pass_completions': 'green',
       'player_pass_attempts': 'green',
       'player_pass_interceptions': 'green',
       'player_pass_longest_completion': 'green',
       'player_rush_yds': 'green',
       'player_rush_attempts': 'green',
       'player_rush_longest': 'green',
       'player_receptions': 'green',
       'player_reception_yds': 'green',
       'player_reception_longest': 'green',
       'player_kicking_points': 'green',
       'player_field_goals': 'green',
       'player_tackles_assists': 'green',
       'player_points': 'green', 
       'player_rebounds': 'green', 
       'player_assists': 'green',
       'player_threes': 'green', 
       'player_double_double': 'green',
       'player_blocks': 'green', 
       'player_steals': 'green', 
       'player_turnovers': 'green', 
       'player_points_rebounds_assists': 'green', 
       'player_points_rebounds': 'green', 
       'player_points_assists': 'green', 
       'player_rebounds_assists': 'green',
       'player_points': 'green',
       'player_power_play_points': 'green',
       'player_blocked_shots' : 'green',
       'player_shots_on_goal' : 'green',
       'player_total_saves': 'green',
       'player_points_alternate':'green',
       'player_rebounds_alternate' :'green', 
       'player_assists_alternate':'green', 
       'player_blocks_alternate':'green', 
       'player_steals_alternate':'green', 
       'player_steals_alternate':'green', 
       'player_points_assists_alternate':'green', 
       'player_points_rebounds_alternate':'green', 
       'player_rebounds_assists_alternate':'green', 
       'player_points_rebounds_assists_alternate':'green',
       'player_power_play_points_alternate' : 'green', 
       'player_goals_alternate' : 'green', 
       'player_shots_on_goal_alternate' : 'green', 
       'player_blocked_shots_alternate' : 'green', 
       'player_total_saves_alternate' : 'green',
       'batter_home_runs': 'green',
       'batter_hits': 'green', 
       'batter_total_bases': 'green', 
       'batter_rbis': 'green', 
       'batter_runs_scored': 'green', 
       'batter_hits_runs_rbis': 'green', 
       'batter_singles': 'green', 
       'batter_doubles': 'green', 
       'batter_triples': 'green', 
       'batter_walks': 'green', 
       'batter_strikeouts': 'green', 
       'batter_stolen_bases': 'green', 
       'pitcher_strikeouts': 'green', 
       'pitcher_hits_allowed': 'green', 
       'pitcher_walks': 'green', 
       'pitcher_earned_runs': 'green', 
       'pitcher_outs': 'green',
       'totals_1st_1_innings' : 'spreads_and_totals', 
       'totals_1st_3_innings' : 'spreads_and_totals', 
       'totals_1st_5_innings' : 'spreads_and_totals', 
       'totals_1st_7_innings' : 'spreads_and_totals',
       'alternate_totals_1st_1_innings' : 'spreads_and_totals',
       'alternate_totals_1st_3_innings' : 'spreads_and_totals',
       'alternate_totals_1st_5_innings' : 'spreads_and_totals',
       'alternate_totals_1st_7_innings' : 'spreads_and_totals',
       'h2h_1st_1_innings' : 'moneyline', 
       'h2h_1st_3_innings' : 'moneyline',
       'h2h_1st_5_innings' : 'moneyline',
       'h2h_1st_7_innings' : 'moneyline', 
       'spreads_1st_1_innings' : 'brown', 
       'spreads_1st_3_innings' : 'brown',
       'spreads_1st_5_innings' : 'brown',
       'spreads_1st_7_innings' : 'brown',
       'alternate_spreads_1st_1_innings' : 'brown',
       'alternate_spreads_1st_3_innings' : 'brown',
       'alternate_spreads_1st_5_innings' : 'brown',
       'alternate_spreads_1st_7_innings' : 'brown',
       'h2h_q1': 'moneyline',
       'h2h_q2': 'moneyline',
       'h2h_q3': 'moneyline',
       'h2h_q4': 'moneyline',
       'h2h_h1': 'moneyline',
       'h2h_h2': 'moneyline',
       'h2h_p1': 'moneyline',
       'h2h_p2': 'moneyline',
       'h2h_p3': 'moneyline',
       'spreads_q1': 'brown',
       'spreads_q2': 'brown',
       'spreads_q3': 'brown', 
       'spreads_q4': 'brown',
       'spreads_h1': 'brown', 
       'spreads_h2': 'brown', 
       'spreads_p1': 'brown', 
       'spreads_p2': 'brown', 
       'spreads_p3': 'brown', 
       'totals_q1': 'spreads_and_totals', 
       'totals_q2': 'spreads_and_totals', 
       'totals_q3': 'spreads_and_totals', 
       'totals_q4': 'spreads_and_totals', 
       'totals_h1': 'spreads_and_totals',
       'totals_h2': 'spreads_and_totals', 
       'totals_p1': 'spreads_and_totals', 
       'totals_p2': 'spreads_and_totals', 
       'totals_p3': 'spreads_and_totals',
       'draw_no_bet': 'moneyline',
       'player_shots_on_target': 'green',
       'player_shots': 'green',
       'alternate_spreads_corners': 'brown', 
       'alternate_totals_corners': 'spreads_and_totals',
       'alternate_spreads_cards': 'brown',
       'alternate_totals_cards': 'spreads_and_totals',
       'alternate_team_totals': 'green',
       'alternate_spreads_q1': "brown",
       'alternate_spreads_q2': "brown",
       'alternate_spreads_q3': "brown",
       'alternate_spreads_q4': "brown",
       'alternate_spreads_h1': "brown",
       'alternate_spreads_h2': "brown",
       "alternate_team_totals_h1": "green",
       "alternate_team_totals_h2": "green",
       "team_totals_h1": "green",
       "team_totals_h2": "green",
       "team_totals_q1": "green",
       "team_totals_q2": "green",
       "team_totals_q3": "green",
       "team_totals_q4": "green",
       "player_pass_tds_alternate": "green",
       "player_pass_yds_alternate": "green",
       "player_rush_yds_alternate": "green",
       "player_rush_reception_yds_alternate": "green",
       "player_reception_yds_alternate": "green",
       "player_receptions_alternate": "green"
       

    }

  def get_list_of_sporting_events(self, sport):
      API_KEY = self.API_KEY
      SPORT = sport
    
      response = requests.get(
         f'https://api.the-odds-api.com/v4/sports/{SPORT}/events/?apiKey={API_KEY}'
                                   )

      if response.status_code != 200:
          raise RuntimeError(response.text)
         #  print(f'Failed to get odds: status_code {response.status_code}, response body {response.text}')
      else:
        
        response_json = response.json()

        event_list = {}

        for item in response_json:
           event_list[item['id']] = item['commence_time']
        
        if len(event_list) == 0:
           time.sleep(3600)
        return event_list
    

  def save_list_of_sporting_events(self, game_ids):

      directory = 'sport_game_id_files'
      
      file_path = os.path.join(directory, f'{self.sport}.json')
      
      try:
         os.makedirs(directory, exist_ok=True)
         
         with open(file_path, 'w') as file:
               json.dump(game_ids, file)
         print("\nGame IDs saved successfully!\n")
      except Exception as e:
         print(f"\nGame IDs could not be saved: {e}")


  def get_list_of_sports(self):
      API_KEY = self.API_KEY

      response = requests.get(
          f'https://api.the-odds-api.com/v4/sports?apiKey={API_KEY}',
          params={
              'api_key': API_KEY,
          }
      )

      if response.status_code != 200:
          raise RuntimeError(response.text)
         #  print(f'Failed to get odds: status_code {response.status_code}, response body {response.text}')
      else:
        response_json = response.json()
        sport_list = []
        for item in response_json:
           sport_list.append(item['key'])
        
        return sport_list
     

  def get_odds(self, sport, game_id, market):
    
      API_KEY = self.API_KEY
      SPORT = sport
      REGIONS = 'us,us2,eu'
      MARKETS = market
      ODDS_FORMAT = 'decimal'
      DATE_FORMAT = 'iso'
      GAME_ID = game_id

      odds_response = requests.get(
          f'https://api.the-odds-api.com/v4/sports/{SPORT}/events/{GAME_ID}/odds?apiKey={API_KEY}&regions={REGIONS}&markets={MARKETS}',
          params={
              'api_key': API_KEY,
              'regions': REGIONS,
              'markets': MARKETS,
              'oddsFormat': ODDS_FORMAT,
              'dateFormat': DATE_FORMAT,
              'events':game_id
          }
      )

      if odds_response.status_code != 200:
          raise RuntimeError(json.loads(odds_response.text)['message'])
         #  print(f'Failed to get odds: status_code {odds_response.status_code}, response body {odds_response.text}')
      else:
        odds_json = odds_response.json()

        return odds_json


  def digest_market_odds(self, sport, game_id, market):
     if sport == 'baseball_mlb':
      logger.info(f'starting digest market odds {datetime.now()} memory: { process.memory_info().rss / 1024 / 1024}')
     
     odds_df = pd.DataFrame(columns=['wagers'])

     odds_df['wagers'] = ''
     odds_df['bet_type'] = ''

     odds = self.get_odds(sport, game_id, market)

     data_list = {}

     for bookie in odds['bookmakers']:
          bookie_column_name = bookie['key']
          for market in bookie['markets']:
            for outcome in market['outcomes']:
               market_key = market['key']

               outcome_name = outcome['name']

               outcome_description = ''

               outcome_point = ''

               full_outcome = market['key'] + '_' + outcome['name']

               if 'point' not in outcome:
                  wager_key = f"{outcome['name']}"
               elif 'description' not in outcome:
                  wager_key = f"{outcome['name']}_{str(outcome['point'])}"
               else:
                  wager_key = f"{outcome['description']}_{outcome['name']}_{str(outcome['point'])}"
              
               if 'description' in outcome:
                  full_outcome += '_' + outcome['description']
                  outcome_description = outcome['description']

               if 'point' in outcome:
                  full_outcome += '_' + str(outcome['point'])
                  outcome_point = str(outcome['point'])

               if full_outcome not in data_list:
                  data_list[full_outcome] = {}
               data_list[full_outcome][bookie_column_name] = outcome['price']
               data_list[full_outcome]['market_key'] = market_key
               data_list[full_outcome]['outcome_name'] = outcome_name
               data_list[full_outcome]['outcome_description'] = outcome_description
               data_list[full_outcome]['outcome_point'] = outcome_point
               data_list[full_outcome]['wagers'] = wager_key

                      
     odds_df = pd.DataFrame(data_list)

     odds_df = odds_df.T.reset_index()
     odds_df = odds_df.rename(columns={'index': 'outcome'})
     odds_df['game_id'] = odds['id']
     odds_df['commence_time'] = odds['commence_time']
   #   odds_df['time_pulled'] = format_time(date)
     odds_df['home_team'] = odds['home_team']
     odds_df['away_team'] = odds['away_team']
     odds_df['sport_title'] = sport
     odds_df['sport_title_display'] = ''
     odds_df['bet_type'] = odds_df['market_key'].map(self.market_type_dict)
     
     odds_df.dropna(subset=['bet_type'], inplace=True)

     odds_df.fillna(0, inplace=True)

     if len(odds_df) > 0:

      cols_to_exclude = ['outcome', 'market_key', 'outcome_name', 'outcome_description', 'outcome_point', 'wagers', 'game_id', 'commence_time', 'home_team', 'away_team', 'sport_title', 'wager_display', 'market_display', 'sport_title_display', 'bet_type']
      
      odds_df['average_market_odds'] = odds_df[[col for col in odds_df.columns if col not in cols_to_exclude]].apply(lambda x: x[x != 0].mean(), axis=1)

      odds_df = self.check_for_bad_data(odds_df, cols_to_exclude)

      odds_df['average_market_odds'] = odds_df[[col for col in odds_df.columns if col not in cols_to_exclude]].apply(lambda x: x[x != 0].mean(), axis=1)

      bettable_books = ['betfair_ex_au', 'betfair_ex_eu', 'betfair_ex_uk', 'betfair_sb_uk', 'betmgm', 'betonlineag', 'betparx', 'betr_au', 'betrivers', 'betus', 'betvictor', 'betway', 'bluebet', 'bovada', 'boylesports', 'casumo', 'coral', 'draftkings', 'espnbet', 'everygame', 'fanduel', 'fliff', 'grosvenor', 'ladbrokes_au', 'ladbrokes_uk', 'leovegas', 'livescorebet', 'livescorebet_eu', 'lowvig', 'matchbook', 'mrgreen', 'mybookieag', 'neds', 'nordicbet', 'paddypower', 'pinnacle', 'playup', 'pointsbetau', 'pointsbetus', 'sisportsbook', 'skybet', 'sport888', 'sportsbet', 'superbook', 'suprabets', 'tipico_us', 'topsport', 'twinspires', 'unibet', 'unibet_eu', 'unibet_uk', 'unibet_us', 'virginbet', 'williamhill', 'williamhill_us', 'windcreek', 'wynnbet']

      selected_columns = [col for col in bettable_books if col in odds_df.columns]

      odds_df['highest_bettable_odds'] = odds_df[selected_columns].max(axis=1)

      odds_df.fillna(0, inplace=True)

      odds_df, arb_df, market_view_df = self.calc_evs(odds_df)

      odds_df.fillna(0, inplace=True)
      arb_df.fillna(0, inplace=True)
      market_view_df.fillna(0, inplace=True)

      odds_df.sort_values(by='ev', ascending=False, inplace=True)

      return_ev_df, return_arb_df, return_market_view_df = [pd.DataFrame() for _ in range(3)]

      odds_df['wager'] = odds_df['wagers']
      arb_df['wager'] = arb_df['wagers']
      market_view_df['wager'] = market_view_df['wagers']



      if len(odds_df) > 0:
         odds_df['sport_title'] = sport
         odds_df['game_id'] = game_id
         odds_df['market'] = market
         odds_df['wager'] = odds_df['wagers']
         odds_df['game_date'] = odds_df['commence_time']
         odds_df['home_team'] =odds['home_team']
         odds_df['away_team'] =odds['away_team']
         odds_df = map_display_data('sport_title', odds_df)
         odds_df = map_display_data('market', odds_df)
         odds_df = map_display_data('wager', odds_df)
         return_ev_df = self.handle_positive_ev_observations(odds_df)

         
      if len(arb_df) > 0:
         arb_df['sport_title'] = sport
         arb_df['game_id'] = game_id
         arb_df['market'] = market
         arb_df['wager'] = arb_df['wagers']
         arb_df['game_date'] = arb_df['commence_time']
         arb_df['home_team'] = odds['home_team']
         arb_df['away_team'] = odds['away_team']
         arb_df = map_display_data('sport_title', arb_df)
         arb_df = map_display_data('market', arb_df)
         arb_df = map_display_data('wager', arb_df)
         return_arb_df = self.handle_arb_observations(arb_df)

      if len(market_view_df) > 0:
         market_view_df['sport_title'] = sport
         market_view_df['game_id'] = game_id
         market_view_df['market'] = market
         market_view_df['wager'] = market_view_df['wagers']
         market_view_df['game_date'] = market_view_df['commence_time']
         market_view_df['home_team'] = odds['home_team']
         market_view_df['away_team'] = odds['away_team']
         market_view_df = map_display_data('sport_title', market_view_df)
         market_view_df = map_display_data('market', market_view_df)
         market_view_df = map_display_data('wager', market_view_df)
         return_market_view_df = self.handle_market_view_observations(market_view_df)

      return return_ev_df, return_arb_df, return_market_view_df
     

  def calc_ev(self, df):

        df['no_vig_prob_1'] = (1 / df['average_market_odds']) / ((1 / df['average_market_odds']) + (1 / df['average_market_odds_other']))
        df['ev'] = ((df['highest_bettable_odds'] - 1) * df['no_vig_prob_1']) - (1 - df['no_vig_prob_1'])
        df['ev'] = df['ev'] * 100
        
        # Additive Method
        df['implied_prob_1'] = 1 / df['average_market_odds']
        df['implied_prob_2'] = 1 / df['average_market_odds_other']
        df['total_vig'] = (df['implied_prob_1'] + df['implied_prob_2']) - 1
        df['no_vig_prob_additive_1'] = df['implied_prob_1'] - (df['total_vig'] / 2)
        df['ev_additive'] = ((df['highest_bettable_odds'] - 1) * df['no_vig_prob_additive_1']) - (1 - df['no_vig_prob_additive_1'])
        df['ev_additive'] = df['ev_additive'] * 100
        df['ev_shin'] = df['ev_additive'] 


        # Shin Method ---- note: shin method is equivalent to additive method for only two outcomes 
      #   df['no_vig_prob_shin'] = shin.calculate_implied_probabilities([df['average_market_odds'], df['average_market_odds_other']])[0]
      #   df['ev_shin'] = ((df['highest_bettable_odds'] - 1) * df['no_vig_prob_shin']) - (1 - df['no_vig_prob_shin'])
      #   df['ev_shin'] = df['ev_shin'] * 100

        # Power Method
        tolerance=1e-6

    # Step 1: Calculate implied probabilities
        p1 = 1 / df['average_market_odds']
        p2 = 1 /df['average_market_odds_other']

    # Step 2: Calculate overround
        overround = p1 + p2 - 1
        

    # Step 3: Calculate return to punter
        r = 1 - overround

    # Step 4: Initial k calculation
        k = math.log(2 * r) / math.log(2)
   

      #   while True:
        # Step 5: Adjust probabilities
        adj_p1 = p1 ** k
        adj_p2 = p2 ** k

      # Check if probabilities sum to 1 within tolerance
        abs_diff = abs(adj_p1 + adj_p2 - 1)


      # Check if the sum of absolute differences is within tolerance
         # if (abs_diff < tolerance):
         #    break

      # Adjust k
        k *= (adj_p1 + adj_p2)



        df['no_vig_prob_power_1'] = adj_p1
        
        df['ev_power'] = ((df['highest_bettable_odds'] - 1) * adj_p1) - adj_p2
        df['ev_power'] = df['ev_power'] * 100

        # Filter for positive EV


        return df


  def calc_arb(self, df):
     df['implied_1'] = 1/df['highest_bettable_odds']
     df['implied_2'] = 1/df['highest_bettable_odds_other']
     df['implied_sum'] = df['implied_1'] + df['implied_2']

     df['arb_perc'] = (1 - df['implied_sum']) / df['implied_sum']

     df = df[df['arb_perc'] > 0]

     return df


  def get_other_side_moneyline(self, df):

      outcome_names = df['outcome_name'].unique().tolist()

      df['other_side_outcome'] = np.where(
       df['outcome_name'] == outcome_names[0],
       df['market_key'] + "_" + outcome_names[1],
       df['market_key'] + "_" + outcome_names[0]
    )

      merged_df = pd.merge(df, df, left_on='outcome', right_on='other_side_outcome', suffixes=('', '_other'))

      return merged_df
  
     
  def get_other_side_brown(self, df):

    outcome_names = df['outcome_name'].unique().tolist()

    df['other_side_outcome'] = np.where(
       df['outcome_name'] == outcome_names[0],
       df['market_key'] + "_" + outcome_names[1] + "_" + (df['outcome_point'].astype(float) * -1).astype(str),
       df['market_key'] + "_" + outcome_names[0] + "_" +(df['outcome_point'].astype(float) * -1).astype(str),
    )

    merged_df = pd.merge(df, df, left_on='outcome', right_on='other_side_outcome', suffixes=('', '_other'))

    return merged_df
  

  def get_other_side_green(self, df):

    outcome_names = df['outcome_name'].unique().tolist()

    df['other_side_outcome'] = np.where(
       df['outcome_name'] == outcome_names[0],
       df['market_key'] + "_" + outcome_names[1] + "_" + df['outcome_description'] + "_" + df['outcome_point'],
       df['market_key'] + "_" + outcome_names[0] + "_" + df['outcome_description'] + "_" + df['outcome_point']
    )

    merged_df = pd.merge(df, df, left_on='outcome', right_on='other_side_outcome', suffixes=('', '_other'))


    return merged_df
    

  def get_other_side_spread_or_total(self, df):
    
    outcome_names = df['outcome_name'].unique().tolist()

    df['other_side_outcome'] = np.where(
       df['outcome_name'] == outcome_names[0],
       df['market_key'] + "_" + outcome_names[1] + "_" + df['outcome_point'],
       df['market_key'] + "_" + outcome_names[0] + "_" + df['outcome_point']
    )

    merged_df = pd.merge(df, df, left_on='outcome', right_on='other_side_outcome', suffixes=('', '_other'))


    return merged_df


  def calc_evs(self, df):
      conditions = [
         (df['bet_type'] == "moneyline"),
         (df['bet_type'] == "brown"),
         (df['bet_type'] == "green"),
         (df['bet_type'] == "spreads_and_totals"),
         (df['bet_type'].isin(["moneyline", "spreads_and_totals", "brown", "green"]) == False)  # default condition
      ]

      functions = [
         self.get_other_side_moneyline,
         self.get_other_side_brown,
         self.get_other_side_green,
         self.get_other_side_spread_or_total,
         lambda x: x 
      ]

      result_dfs = []

      for condition, func in zip(conditions, functions):
         subset_df = df.loc[condition]
         try:
            result_df = func(subset_df)
            result_dfs.append(result_df)
         except IndexError:
            continue

      final_result = pd.concat(result_dfs, ignore_index=True)

      df = final_result.apply(self.calc_ev, axis=1)
      ev_df = df[(df['ev'] > 0) | (df['ev_additive'] > 0)  | (df['ev_power'] > 0)]

      arb_df = self.calc_arb(final_result)
     
      return ev_df, arb_df, final_result
  

  def find_matching_columns(self, row, bettable_books):
    matching_cols = [col.title() for col in bettable_books if row[col] == row['highest_bettable_odds']]
    return list(set(matching_cols))
  

  def find_matching_columns_other(self, row, bettable_books):
    matching_cols = [col.split("_other")[0].title() for col in bettable_books if row[col] == row['highest_bettable_odds_other']]
    return list(set(matching_cols))


  def handle_positive_ev_observations(self, df):
     
     bettable_books_full = ['betfair_ex_au', 'betfair_ex_eu', 'betfair_ex_uk', 'betfair_sb_uk', 'betmgm', 'betonlineag', 'betparx', 'betr_au', 'betrivers', 'betus', 'betvictor', 'betway', 'bluebet', 'bovada', 'boylesports', 'casumo', 'draftkings', 'espnbet', 'everygame', 'fanduel', 'fliff', 'grosvenor', 'ladbrokes_au', 'ladbrokes_uk', 'leovegas', 'livescorebet', 'livescorebet_eu', 'lowvig', 'matchbook', 'mrgreen', 'mybookieag', 'neds', 'nordicbet', 'paddypower', 'pinnacle', 'playup', 'pointsbetau', 'pointsbetus', 'sisportsbook', 'skybet', 'sport888', 'sportsbet', 'superbook', 'suprabets', 'tipico_us', 'topsport', 'twinspires', 'unibet', 'unibet_eu', 'unibet_uk', 'unibet_us', 'virginbet', 'williamhill', 'williamhill_us', 'windcreek', 'wynnbet'
        ]

     bettable_books = [col for col in df.columns if col in bettable_books_full]

     df['sportsbooks_used'] = df.apply(lambda row: self.find_matching_columns(row, bettable_books), axis=1)

     df['snapshot_time'] = pd.to_datetime(datetime.now())

   #   full_df = self.read_cached_df(self.ev_cache_paths[self.sport])
   
   #   all_columns = full_df.columns

   #   df = df.reindex(columns=all_columns, fill_value=0)

     return df


  def handle_arb_observations(self, df):
     
     bettable_books_full = ['betmgm', 'betonlineag', 'betparx', 'betr_au', 'betrivers', 'betus', 'betvictor', 'betway', 'bluebet', 'bovada', 'boylesports', 'casumo', 'coral', 'draftkings', 'espnbet', 'everygame', 'fanduel', 'fliff', 'grosvenor', 'ladbrokes_au', 'ladbrokes_uk', 'leovegas', 'mrgreen', 'mybookieag', 'neds', 'nordicbet', 'paddypower', 'pinnacle', 'playup', 'pointsbetau', 'pointsbetus', 'sisportsbook', 'skybet', 'sport888', 'sportsbet', 'superbook', 'suprabets', 'tipico_us', 'topsport', 'twinspires', 'unibet', 'unibet_eu', 'unibet_uk', 'unibet_us', 'virginbet', 'williamhill', 'williamhill_us', 'windcreek', 'wynnbet']

     bettable_books = [col for col in df.columns if col in bettable_books_full]

     bettable_books_other = [col + "_other" for col in df.columns if col in bettable_books_full]

     df['sportsbooks_used'] = df.apply(lambda row: self.find_matching_columns(row, bettable_books), axis=1)

     df['sportsbooks_used_other'] = df.apply(lambda row: self.find_matching_columns_other(row, bettable_books_other), axis=1)

     df = df[df['sportsbooks_used'].apply(lambda x: len(x) > 0)]

     df = df[df['sportsbooks_used_other'].apply(lambda x: len(x) > 0)]

     df['snapshot_time'] = pd.to_datetime(datetime.now())

   #   full_df = self.read_cached_df(self.arb_cache_paths[self.sport])

   #   all_columns = full_df.columns

   #   df = df.reindex(columns=all_columns, fill_value=0)

     return df
  

  def handle_market_view_observations(self, df):
     
     bettable_books_full = ['betmgm', 'betonlineag', 'betparx', 'betr_au', 'betrivers', 'betus', 'betvictor', 'betway', 'bluebet', 'bovada', 'boylesports', 'casumo', 'coral', 'draftkings', 'espnbet', 'everygame', 'fanduel', 'fliff', 'grosvenor', 'ladbrokes_au', 'ladbrokes_uk', 'leovegas', 'mrgreen', 'mybookieag', 'neds', 'nordicbet', 'paddypower', 'pinnacle', 'playup', 'pointsbetau', 'pointsbetus', 'sisportsbook', 'skybet', 'sport888', 'sportsbet', 'superbook', 'suprabets', 'tipico_us', 'topsport', 'twinspires', 'unibet', 'unibet_eu', 'unibet_uk', 'unibet_us', 'virginbet', 'williamhill', 'williamhill_us', 'windcreek', 'wynnbet']

     bettable_books = [col for col in df.columns if col in bettable_books_full]

     bettable_books_other = [col + "_other" for col in df.columns if col in bettable_books_full]

     df['sportsbooks_used'] = df.apply(lambda row: self.find_matching_columns(row, bettable_books), axis=1)

     df['sportsbooks_used_other'] = df.apply(lambda row: self.find_matching_columns_other(row, bettable_books_other), axis=1)

     df = df[df['sportsbooks_used'].apply(lambda x: len(x) > 0)]

     df = df[df['sportsbooks_used_other'].apply(lambda x: len(x) > 0)]

     df['snapshot_time'] = pd.to_datetime(datetime.now())

     return df


  def clear_market_view_observations(self, game_id, market):

     stored_data = self.read_cached_df(self.market_view_cache_paths[self.sport])

     game_id_market = game_id + market

     stored_data_without_market = stored_data[stored_data['game_id_market'] != game_id_market]

     self.write_to_cache(stored_data_without_market, self.market_view_cache_paths[self.sport])

     return
     

  def remove_event_obs(self, game_id):

   df = self.read_cached_df(self.ev_cache_paths[self.sport])
   
   df = df[df['game_id'] != game_id]

   self.write_to_cache(df, self.ev_cache_paths[self.sport])

   df = self.read_cached_df(self.arb_cache_paths[self.sport])
   

   df = df[df['game_id'] != game_id]

   self.write_to_cache(df, self.arb_cache_paths[self.sport])

   return 
  

  def replace_dfs(self, ev_df, arb_df, market_view_df):

     self.write_to_cache(ev_df, self.ev_cache_paths[self.sport])

     self.write_to_cache(arb_df, self.arb_cache_paths[self.sport])

     self.write_to_cache(market_view_df, self.market_view_cache_paths[self.sport])
      
     return


  def game_id_has_cache(self, cache_path):
     return redis_client.exists(cache_path)
 

  def set_cache_with_extended_expiration(self, game_id, cache_path, df, date_format = "%Y-%m-%dT%H:%M:%SZ"
):
    """
    Set a cache entry with an expiration date extended by 6 hours.

    :param redis_client: Redis client instance
    :param cache_name: Name of the cache
    :param value: Value to store in the cache
    :param expiration_date_str: Expiration date as a string in ISO 8601 format
    :param date_format: The format of the expiration_date_str if not ISO 8601
    """
    # Parse the expiration date string to a datetime object
    expiration_date_str = self.event_dict[game_id]
    
    expiration_utc = datetime.strptime(expiration_date_str, date_format).replace(tzinfo=timezone.utc)
    
    # Add 6 hours to the expiration date
    extended_expiration_utc = expiration_utc + timedelta(hours=6)
    
    # Get the current time in UTC
    now_utc = datetime.now(timezone.utc)
    
    # Calculate the number of seconds until the extended expiration
    expiration_seconds = int((extended_expiration_utc - now_utc).total_seconds())
    
    if expiration_seconds <= 0:
        raise ValueError("Expiration time must be in the future.")
    
    # Set the cache entry with the new expiration time
    serialized_df = df.to_json()
    redis_client.set(cache_path, serialized_df, ex=expiration_seconds)
     

  def run(self, sport, game_ids):
      
      markets =  ",".join(self.markets_sports.get(sport))
      
      for game_id in game_ids:
         
         ev_df, arb_df, market_view_df = self.digest_market_odds(sport, game_id, markets)

         if not self.game_id_has_cache(f"pos_ev_{game_id}"):
            self.set_cache_with_extended_expiration(game_id=game_id, df = ev_df, cache_path=  f'pos_ev_{game_id}')
         else:
            self.write_to_cache(df = ev_df, cache_path = f'pos_ev_{game_id}')

         if not self.game_id_has_cache(f"arb_{game_id}"):
            self.set_cache_with_extended_expiration(game_id=game_id, df = arb_df, cache_path=  f"arb_{game_id}")
         else:
            self.write_to_cache(df = arb_df, cache_path = f"arb_{game_id}")

         if not self.game_id_has_cache(f"market_view_{game_id}"):
            self.set_cache_with_extended_expiration(game_id=game_id, df = market_view_df, cache_path=  f"market_view_{game_id}")
         else:
            self.write_to_cache(df = market_view_df, cache_path = f"market_view_{game_id}")

      return
  

  def split_into_chunks(self, data, num_chunks):
    """Splits the data into `num_chunks` roughly equal chunks."""
    avg_chunk_size, remainder = divmod(len(data), num_chunks)
    chunks = []
    start = 0

    for i in range(num_chunks):
        end = start + avg_chunk_size + (1 if i < remainder else 0)
        chunks.append(data[start:end])
        start = end

    return chunks
   
   
  def make_live_dash_data(self):
     # for each sport
     for sport in self.sports:
        
        event_dict = self.get_list_of_sporting_events(sport)

        self.event_dict = event_dict

        self.save_list_of_sporting_events(event_dict)

        event_list = list(event_dict.keys())

        threads = []
        num_threads = 8

        game_ids_for_threads = self.split_into_chunks(event_list, num_threads)

        for index in range(num_threads):
            thread = threading.Thread(target=self.run, args=(sport, game_ids_for_threads[index]))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

     logger.info("Processes completed, going to sleep for 1 min...")
     time.sleep(60)
     
     return


  def check_for_bad_data(self, df, cols_to_exclude):

   # Check to make sure some oddsarent 500% greater one place than the market average
   for col in df.columns:
      if  col not in cols_to_exclude:
         try:  
            mask = df[col].astype(float) > df['average_market_odds'].astype(float) * 5
            df.loc[mask, col] = np.nan
         except:
            print("Error occured checking data for wild values...")

   df = df.drop(columns=['average_market_odds'])
   return df
  

  def write_to_cache(self, df, cache_path):
    if not df.index.is_unique:
        df = df.reset_index(drop=True)
    serialized_df = df.to_json()
    redis_client.set(cache_path, serialized_df)
    print(f"df written to: {cache_path}")


  def read_cached_df(self, path):
    serialized_df = redis_client.get(path)
    if serialized_df:
      serialized_df = serialized_df.decode('utf-8')
      cached_df = pd.read_json(serialized_df)
      return cached_df
    else:
      raise FileNotFoundError(f"No cached DataFrame found at path: {path}")