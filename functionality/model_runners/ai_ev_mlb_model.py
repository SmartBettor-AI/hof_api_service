import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import random
from sklearn.preprocessing import StandardScaler, OneHotEncoder
import torch
from torch import nn
from torch.utils.data import DataLoader
from sklearn.metrics import roc_auc_score
import plotly.express as px
import pickle
from datetime import datetime, time
import os 
import warnings
import time
import redis
redis_client = redis.Redis(host='localhost', port=6379, db=0)
from model_db_manager import DBManager
import pandas as pd
from sqlalchemy import inspect, types
from sqlalchemy.exc import SQLAlchemyError

warnings.filterwarnings("ignore")

class NoNonEmptyDataFramesError(Exception):
    """Exception raised when no non-empty DataFrames are found."""
    
    def __init__(self, message="No non-empty DataFrames available."):
        super().__init__(message)
        

def read_cached_df(path):
    serialized_df = redis_client.get(path)
    if serialized_df:
      serialized_df = serialized_df.decode('utf-8')
      cached_df = pd.read_json(serialized_df)
      return cached_df
    else:
      raise FileNotFoundError(f"No cached DataFrame found at path: {path}")
    


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
     
     elif column_name == 'market_key':
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
     


# TODO: load the params and filter by odds and shit according to saved params not hardcoded
class PreProcessing():

    def __init__(self, sport = None, live_or_pregame = None):
        
        self.sport = sport

        self.live_or_pregame = live_or_pregame

        self.MIN_AVERAGE_MARKET_ODDS = 1.2
        self.MAX_AVERAGE_MARKET_ODDS = 4

        self.MIN_BEST_ODDS = -1000
        self.MAX_BEST_ODDS = 9999

        self.MIN_EV = 0
        self.MAX_EV = 1000

        self.team_to_conference = {
            'New York Yankees': 'American League',
            'Boston Red Sox': 'American League',
            'Tampa Bay Rays': 'American League',
            'Toronto Blue Jays': 'American League',
            'Baltimore Orioles': 'American League',
            'Chicago White Sox': 'American League',
            'Cleveland Guardians': 'American League',
            'Detroit Tigers': 'American League',
            'Kansas City Royals': 'American League',
            'Minnesota Twins': 'American League',
            'Houston Astros': 'American League',
            'Los Angeles Angels': 'American League',
            'Oakland Athletics': 'American League',
            'Seattle Mariners': 'American League',
            'Texas Rangers': 'American League',
            'Atlanta Braves': 'National League',
            'Miami Marlins': 'National League',
            'New York Mets': 'National League',
            'Philadelphia Phillies': 'National League',
            'Washington Nationals': 'National League',
            'Chicago Cubs': 'National League',
            'Cincinnati Reds': 'National League',
            'Milwaukee Brewers': 'National League',
            'Pittsburgh Pirates': 'National League',
            'St. Louis Cardinals': 'National League',
            'Arizona Diamondbacks': 'National League',
            'Colorado Rockies': 'National League',
            'Los Angeles Dodgers': 'National League',
            'San Diego Padres': 'National League',
            'San Francisco Giants': 'National League',
            'Buffalo Bills': 'AFC',
            'Miami Dolphins': 'AFC',
            'New England Patriots': 'AFC',
            'New York Jets': 'AFC',
            'Baltimore Ravens': 'AFC',
            'Cincinnati Bengals': 'AFC',
            'Cleveland Browns': 'AFC',
            'Pittsburgh Steelers': 'AFC',
            'Houston Texans': 'AFC',
            'Indianapolis Colts': 'AFC',
            'Jacksonville Jaguars': 'AFC',
            'Tennessee Titans': 'AFC',
            'Denver Broncos': 'AFC',
            'Kansas City Chiefs': 'AFC',
            'Las Vegas Raiders': 'AFC',
            'Los Angeles Chargers': 'AFC',
            'Dallas Cowboys': 'NFC',
            'New York Giants': 'NFC',
            'Philadelphia Eagles': 'NFC',
            'Washington Commanders': 'NFC',
            'Chicago Bears': 'NFC',
            'Detroit Lions': 'NFC',
            'Green Bay Packers': 'NFC',
            'Minnesota Vikings': 'NFC',
            'Atlanta Falcons': 'NFC',
            'Carolina Panthers': 'NFC',
            'New Orleans Saints': 'NFC',
            'Tampa Bay Buccaneers': 'NFC',
            'Arizona Cardinals': 'NFC',
            'Los Angeles Rams': 'NFC',
            'San Francisco 49ers': 'NFC',
            'Seattle Seahawks': 'NFC'
        }


        self.team_to_division = {
            'New York Yankees': 'AL East',
            'Boston Red Sox': 'AL East',
            'Tampa Bay Rays': 'AL East',
            'Toronto Blue Jays': 'AL East',
            'Baltimore Orioles': 'AL East',
            'Chicago White Sox': 'AL Central',
            'Cleveland Guardians': 'AL Central',
            'Detroit Tigers': 'AL Central',
            'Kansas City Royals': 'AL Central',
            'Minnesota Twins': 'AL Central',
            'Houston Astros': 'AL West',
            'Los Angeles Angels': 'AL West',
            'Oakland Athletics': 'AL West',
            'Seattle Mariners': 'AL West',
            'Texas Rangers': 'AL West',
            'Atlanta Braves': 'NL East',
            'Miami Marlins': 'NL East',
            'New York Mets': 'NL East',
            'Philadelphia Phillies': 'NL East',
            'Washington Nationals': 'NL East',
            'Chicago Cubs': 'NL Central',
            'Cincinnati Reds': 'NL Central',
            'Milwaukee Brewers': 'NL Central',
            'Pittsburgh Pirates': 'NL Central',
            'St. Louis Cardinals': 'NL Central',
            'Arizona Diamondbacks': 'NL West',
            'Colorado Rockies': 'NL West',
            'Los Angeles Dodgers': 'NL West',
            'San Diego Padres': 'NL West',
            'San Francisco Giants': 'NL West',
            'Buffalo Bills': 'AFC East',
            'Miami Dolphins': 'AFC East',
            'New England Patriots': 'AFC East',
            'New York Jets': 'AFC East',
            'Baltimore Ravens': 'AFC North',
            'Cincinnati Bengals': 'AFC North',
            'Cleveland Browns': 'AFC North',
            'Pittsburgh Steelers': 'AFC North',
            'Houston Texans': 'AFC South',
            'Indianapolis Colts': 'AFC South',
            'Jacksonville Jaguars': 'AFC South',
            'Tennessee Titans': 'AFC South',
            'Denver Broncos': 'AFC West',
            'Kansas City Chiefs': 'AFC West',
            'Las Vegas Raiders': 'AFC West',
            'Los Angeles Chargers': 'AFC West',
            'Dallas Cowboys': 'NFC East',
            'New York Giants': 'NFC East',
            'Philadelphia Eagles': 'NFC East',
            'Washington Commanders': 'NFC East',
            'Chicago Bears': 'NFC North',
            'Detroit Lions': 'NFC North',
            'Green Bay Packers': 'NFC North',
            'Minnesota Vikings': 'NFC North',
            'Atlanta Falcons': 'NFC South',
            'Carolina Panthers': 'NFC South',
            'New Orleans Saints': 'NFC South',
            'Tampa Bay Buccaneers': 'NFC South',
            'Arizona Cardinals': 'NFC West',
            'Los Angeles Rams': 'NFC West',
            'San Francisco 49ers': 'NFC West',
            'Seattle Seahawks': 'NFC West'
        }


    def split_column_ev(self, df, col_to_split):

        for col in ['outcome_name', 'outcome_description', 'outcome_point', 'outcome_type']:
            try:
                df.drop(col, axis=1, inplace=True)
            except:
                pass

        # Split the column and expand into multiple columns
        split_cols = df[col_to_split].str.split('_', n=24, expand=True)

        # Create a DataFrame to store split values
        split_cols.columns = ['split_' + str(i) for i in range(len(split_cols.columns))]

        # Define the function to assign values based on the length of split parts
        def assign_values(row):

            split_len = row.notna().sum()

            if split_len == 1:
                row['outcome_name'] = row['split_0']
            elif split_len == 2:
                if row['split_0'] in ['Over', 'Under']:
                    row['outcome_type'] = row['split_0']
                    row['outcome_point'] = row['split_1']
                else:
                    row['outcome_name'] = row['split_0']
                    row['outcome_point'] = row['split_1']
            elif split_len == 3:
                row['outcome_type'] = row['split_1']
                row['outcome_name'] = row['split_0']
                row['outcome_point'] = row['split_2']
            return row

        split_cols = split_cols.apply(assign_values, axis=1)

        df = pd.concat([df, split_cols], axis=1)
        
        # Drop original column and any unnecessary columns
        # df = df.drop(columns=[col_to_split] + split_cols.columns.tolist())

        return df


    def fit_ev_to_graded_odds_schema(self, batch):

        batch = self.split_column_ev(batch, 'wager')

        if 'average_market_odds_old' not in batch.columns:
          batch['average_market_odds_old'] = batch['average_market_odds'].copy()


        batch['time_pulled'] = batch['snapshot_time'].copy()

        if not 'market_key' in batch.columns.tolist():
            batch['market_key'] = batch['market'].copy()

        batch['commence_time'] = pd.to_datetime(batch['game_date'], format='%Y-%m-%dT%H:%M:%SZ').dt.strftime('%m/%d/%y %H:%M')

        ev_odds_cols = [
                "betonlineag", "betmgm", "betrivers", "betus", "bovada", "draftkings", "fanduel", "lowvig",
                "mybookieag", "pointsbetus", "superbook", "hardrockbet", 'ballybet', 'gtbets',  "twinspires", "unibet_us", "williamhill_us", "wynnbet", 
                "betparx", "espnbet", "fliff", "sisportsbook", "tipico_us", "windcreek", "betfair_ex_uk",
                "betfair_sb_uk", "betvictor", "betway", "boylesports", "casumo", "coral", "grosvenor",
                "ladbrokes_uk", "leovegas", "livescorebet", "matchbook", "mrgreen", "paddypower", "skybet",
                "unibet_uk", "virginbet", "williamhill", "onexbet", "sport888", "betclic", "betfair_ex_eu",
                "betsson", "coolbet", "everygame", "livescorebet_eu", "marathonbet", "nordicbet", "pinnacle",
                "suprabets", "unibet_eu", "betfair_ex_au", "betr_au", "bluebet", "ladbrokes_au", "neds",
                "playup", "pointsbetau", "sportsbet", "tab", "topsport", "unibet",'betanysports'
         ]
        
        ev_odds_cols_other = [col + "_other" for col in ev_odds_cols]
        
        ev_odds_cols_other_X = [col + "_other_X" for col in ev_odds_cols]
        
        for column_title in ev_odds_cols:
            try:
                batch[column_title + "_1_odds"] = batch[column_title].copy()
            except Exception as e:
                print(e)


        cols_to_append = ['sport_title', 'average_market_odds', 'highest_bettable_odds_other_X', 'highest_bettable_odds_other', 'other_average_market_odds', 'no_vig_prob_1', 'ev', 'snapshot_time', 'game_date', 'sport_title_display', 'sport_league_display', 'market_display', 'wager_display', 'wager_display_other', 'market', 'value', 'split_0', 'split_1', 'split_2', 'bet_type', 'other_side_outcome','outcome_other', 'market_key_other', 'outcome_name_other', 'outcome_description_other', 'outcome_point_other', 'hardrockbet_other', 'game_id_other', 'commence_time_other', 'home_team_other', 'away_team_other', 'sport_title_other', 'sport_title_display_other', 'bet_type_other', 'average_market_odds_other', 'other_side_outcome_other', 'wagers', 'Unnamed: 0']

        columns_to_drop = ev_odds_cols_other + ev_odds_cols_other_X + cols_to_append + ev_odds_cols

        for col in columns_to_drop:
            try:
                batch.drop(col, axis=1, inplace=True)
            except:
                pass

        return batch
  

    def replace_missing_vals(self, df):
            
        odds_columns = [col for col in df.columns if 'odds' in col]
        point_columns = [col for col in df.columns if 'point' in col and 'time' not in col]

        for col in odds_columns:
            df[col] = df[col].replace(np.nan, 0)
            df[col] = df[col].astype('float64')

        for col in point_columns:
            df[col] = df[col].replace(np.nan, 0)
            df[col] = df[col].astype('float64')

        return df
    

    def make_highest_bettable_odds(self, df):
            subset_columns = [col for col in df.columns]

            odds_cols = [col for col in subset_columns if '_1_odds' in col]

            # Calculate the maximum odds for each row
            df['highest_bettable_odds'] = df[odds_cols].max(axis=1)

            return df
    

    def assign_bet_type_colors(self, df):
        
        market_type_dict = {
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

        df['bet_type'] = df['market_key'].map(market_type_dict)

        return df
         

    def get_other_side_moneyline(self, df):

      outcome_names = df['outcome_name'].unique().tolist()

      df['outcome_plus_time'] = df['outcome'] + '_' + df['time_pulled'].astype(str)

      df['other_side_outcome'] = np.where(
       df['outcome_name'] == outcome_names[0],
       df['market_key'] + "_" + outcome_names[1] + '_' + df['time_pulled'].astype(str),
       df['market_key'] + "_" + outcome_names[0] + '_' + df['time_pulled'].astype(str)
    )

      merged_df = pd.merge(df, df, left_on='outcome_plus_time', right_on='other_side_outcome', suffixes=('', '_other'))

      return merged_df
  

    def get_other_side_brown(self, df):

        outcome_names = df['outcome_name'].unique().tolist()

        df['outcome_plus_time'] = df['outcome'] + '_' + df['time_pulled'].astype(str)


        df['other_side_outcome'] = np.where(
        df['outcome_name'] == outcome_names[0],
        df['market_key'] + "_" + outcome_names[1] + "_" + (df['outcome_point'].astype(float) * -1).astype(str) + '_' + df['time_pulled'].astype(str),
        df['market_key'] + "_" + outcome_names[0] + "_" +(df['outcome_point'].astype(float) * -1).astype(str) + '_' + df['time_pulled'].astype(str),
        )

        merged_df = pd.merge(df, df, left_on='outcome_plus_time', right_on='other_side_outcome', suffixes=('', '_other'))

        return merged_df
  

    def get_other_side_green(self, df):

        outcome_names = df['outcome_type'].unique().tolist()

        df['outcome_plus_time'] = df['outcome'] + '_' + df['time_pulled'].astype(str)

        df['other_side_outcome'] = np.where(
            df['outcome_type'] == outcome_names[0],
            df['market_key'] + "_" + outcome_names[1] + '_' + df['outcome_name'] + "_" + df['outcome_point'].astype(str) + '_' + df['time_pulled'].astype(str),
            df['market_key'] + "_" + outcome_names[0] + "_" + df['outcome_name'] + "_" + df['outcome_point'].astype(str) + '_' + df['time_pulled'].astype(str)
        )


        merged_df = pd.merge(df, df, left_on='outcome_plus_time', right_on='other_side_outcome', suffixes=('', '_other'))

        return merged_df
    
    # need to re-work this
    def get_other_side_spread_or_total(self, df):
        
        outcome_names = df['outcome_name'].unique().tolist()

        df['outcome_plus_time'] = df['outcome'] + '_' + df['time_pulled'].astype(str)


        df['other_side_outcome'] = np.where(
            df['outcome_name'] == outcome_names[0],
            df['market_key'] + "_" + outcome_names[1] + "_" + df['outcome_point']+ '_' + df['time_pulled'].astype(str),
            df['market_key'] + "_" + outcome_names[0] + "_" + df['outcome_point']+ '_' + df['time_pulled'].astype(str)
        )

        merged_df = pd.merge(df, df, left_on='outcome_plus_time', right_on='other_side_outcome', suffixes=('', '_other'))


        return merged_df


    def calc_ev(self, df):
        # Multiplicative method
        df['average_market_odds'] = df['average_market_odds_old']
        df['average_market_odds_other'] = df['average_market_odds_old_other']
        
        df['no_vig_prob_1'] = (1 / df['average_market_odds']) / ((1 / df['average_market_odds']) + (1 / df['average_market_odds_other']))
        df['ev'] = ((df['highest_bettable_odds'] - 1) * df['no_vig_prob_1']) - (1 - df['no_vig_prob_1'])
        df['ev'] *= 100
        
        # Additive Method
        df['implied_prob_1'] = 1 / df['average_market_odds']
        df['implied_prob_2'] = 1 / df['average_market_odds_other']
        df['total_vig'] = (df['implied_prob_1'] + df['implied_prob_2']) - 1
        df['no_vig_prob_additive_1'] = df['implied_prob_1'] - (df['total_vig'] / 2)
        df['ev_additive'] = ((df['highest_bettable_odds'] - 1) * df['no_vig_prob_additive_1']) - (1 - df['no_vig_prob_additive_1'])
        df['ev_additive'] *= 100

        return df


    def make_evs(self, df):
      
      df = self.assign_bet_type_colors(df)

      conditions = [
         (df['bet_type'] == "moneyline"),
         (df['bet_type'] == "brown"),
         (df['bet_type'] == "green"),
         (df['bet_type'] == "spreads_and_totals"),
         (df['bet_type'].isin(["moneyline", "spreads_and_totals", "brown", "green"]) == False)
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


      non_empty_dfs = [df for df in result_dfs if not df.empty]

      if non_empty_dfs:  # Check if the list is not empty
            final_result = pd.concat(non_empty_dfs, ignore_index=True)

            df = self.calc_ev(final_result)

      else:
            raise NoNonEmptyDataFramesError()

      return df


    def make_average_market_odds_old(self, df):
        # calculates the average market odds for all of the odds we have (including old odds)
        odds_columns = [col for col in df.columns if col.endswith('_odds')]
        odds_df = df[odds_columns]
        df_array = odds_df.values

        # Create mask for values greater than 0.5 (this masks out missing values )
        mask = df_array > 0.5

        # Apply mask and calculate row-wise average
        row_avg = np.nanmean(np.where(mask, df_array, np.nan), axis=1)

        df['average_market_odds_old'] = row_avg

        return df
    
    
    def make_odds_std_dev(self, df):

         std_dev_columns = [col for col in df.columns if '_odds' in col and '_other' not in col and col != 'average_market_odds_old' and col != 'average_market_odds' and col != 'highest_bettable_odds']

         std_dev_columns_other = [col for col in df.columns if '_odds_other' in col and col != 'average_market_odds_old_other' and col != 'average_market_odds_other' and col != 'highest_bettable_odds_other']

         df['std_dev'] = df[std_dev_columns].std(axis=1, skipna=True)

         df['std_dev_other'] = df[std_dev_columns_other].std(axis=1, skipna=True)

         return df


    def make_minutes_since_commence(self, df):
        df['minutes_since_commence'] = (df['time_pulled'] - df['commence_time']).dt.total_seconds() / 60
        return df


    def filter_by_average_market_odds(self, df, MIN_AVERAGE_MARKET_ODDS, MAX_AVERAGE_MARKET_ODDS):
            df = df[df['average_market_odds_old'] >= MIN_AVERAGE_MARKET_ODDS]
            df = df[df['average_market_odds_old'] <= MAX_AVERAGE_MARKET_ODDS]
            return df


    def filter_by_ev_thresh(self, df, MIN_EV, MAX_EV):        

            df = df[(df['ev'] >= MIN_EV) | (df['ev_additive'] >= MIN_EV)]
            df = df[(df['ev'] <= MAX_EV) | (df['ev_additive'] <= MAX_EV)]

            # df = df.drop(columns=['ev'], axis='columns')

            return df


    def filter_by_best_odds(self, df, MIN_BEST_ODDS, MAX_BEST_ODDS):
            df = df[df['highest_bettable_odds'] >= MIN_BEST_ODDS]
            df = df[df['highest_bettable_odds'] <= MAX_BEST_ODDS]

            return df


    def set_new_train_data(self, df_chunk):
         
         df_chunk['commence_time'] = pd.to_datetime(df_chunk['commence_time'])
         df_chunk['day_of_week'] = df_chunk['commence_time'].dt.strftime('%a')
         df_chunk['hour_of_start'] = df_chunk['commence_time'].dt.hour.astype(str)


         if self.live_or_pregame == 'pregame':
             df_chunk = df_chunk[df_chunk['commence_time'] > df_chunk['time_pulled']]
         if self.live_or_pregame == 'live':
             df_chunk = df_chunk[df_chunk['commence_time'] < df_chunk['time_pulled']]
             
         time_columns = [col for col in df_chunk.columns if 'time' in col]

         time_columns.remove('commence_time')

         for col in time_columns:
               df_chunk[col] = pd.to_datetime(df_chunk[col], errors='coerce')

         working_df = self.replace_missing_vals(df_chunk)

         working_df = self.make_highest_bettable_odds(working_df)

        #  working_df = self.make_average_market_odds_old(working_df)

         working_df = self.make_minutes_since_commence(working_df)

         working_df = self.make_evs(working_df)

         working_df = self.make_odds_std_dev(working_df)
         
         working_df_copy = working_df.copy()

         working_df = working_df_copy.copy()

         working_df = self.filter_by_average_market_odds(working_df, self.MIN_AVERAGE_MARKET_ODDS, self.MAX_AVERAGE_MARKET_ODDS)

         working_df = self.filter_by_ev_thresh(working_df, self.MIN_EV, self.MAX_EV)

         working_df = self.filter_by_best_odds(working_df, self.MIN_BEST_ODDS, self.MAX_BEST_ODDS)
         
         working_df['day_or_night'] = working_df['hour_of_start'].apply(lambda x: 'night' if int(x) > 6 else 'day')

         working_df['home_team_conference'] = working_df['home_team'].map(self.team_to_conference)
         working_df['away_team_conference'] = working_df['away_team'].map(self.team_to_conference)
         working_df['home_team_division'] = working_df['home_team'].map(self.team_to_division)
         working_df['away_team_division'] = working_df['away_team'].map(self.team_to_division)


         return working_df

     
    def run(self, df):

        if not df.empty:

          schema_df = self.fit_ev_to_graded_odds_schema(df)

          schema_df = self.set_new_train_data(schema_df)

          schema_df.to_csv("SCHEMA_df.csv")

          return schema_df


class DataLoaderMaker():

  def __init__(self, df, sport = None, name = None):
    self.sport = sport
    self.df = df
    self.scaler = self.load_scaler()
    self.encoder = self.load_encoder()


  def load_scaler(self):
     with open(f"models/scalers/mlb_pregame_scaler.pkl", 'rb') as file:
      return pickle.load(file)
     
   
  def load_encoder(self):
     with open(f"models/encoders/mlb_pregame_encoder.pkl", 'rb') as file:
      return pickle.load(file)


  def standardize_numerical_values(self, df):

        categorical_columns = ['home_team', 'away_team', 'day_of_week', 'hour_of_start','home_team_division', 'away_team_division', 'home_team_conference', 'away_team_conference', 'day_or_night', 'market_key', 'outcome_type']

        numerical_columns = ['betrivers_1_odds', 'fanduel_1_odds', 'unibet_us_1_odds', 'betmgm_1_odds', 'draftkings_1_odds', 'williamhill_us_1_odds', 'bovada_1_odds', 'pointsbetus_1_odds', 'betonlineag_1_odds', 'ev', 'std_dev', 'ev_additive', 'highest_bettable_odds', 'average_market_odds_old', 'minutes_since_commence']

        for col in numerical_columns:
          if col not in df.columns:
              df[col] = 0
        
        numerical_full = df[numerical_columns]
        
        print("\nNumerical data types below: ")
        print(numerical_full.dtypes)
        print("--------------------")

        scaled_data = self.scaler.transform(numerical_full)

        return scaled_data
  

  def encode_categorical_variables(self, df):
      
        categorical_columns = ['home_team', 'away_team', 'day_of_week', 'hour_of_start','home_team_division', 'away_team_division', 'home_team_conference', 'away_team_conference', 'day_or_night', 'market_key', 'outcome_type']

        encoded = np.empty((len(df), 0))

        encoder_obj = self.encoder

        for col in categorical_columns:

            col_encoder = encoder_obj[col]

            col_data = df[[col]].astype(str)

            encoded_columns = col_encoder.transform(col_data)

            encoded = np.hstack((encoded, encoded_columns))

        return encoded


  def make_data_loaders(self):

        try:

          scaled_train_numerical_data = self.standardize_numerical_values(self.df)

          encoded_train_data = self.encode_categorical_variables(self.df)

          final_train_data_numpy = np.hstack((scaled_train_numerical_data, encoded_train_data))

          return torch.tensor(final_train_data_numpy.astype(np.float32))

        except Exception as e:
          print(f"\nError with process:")
          print(e)
 

class Model():
    
    def __init__(self, df, pred_tensor, name):
        self.display_df = df
        self.pred_tensor = pred_tensor
        self.name = name
        self.model = self.load_model()
        self.pred_thresh = self.load_pred_thresh()


    def load_model(self):
          
          loaded_model = torch.load(f'models/model_objs/{self.name}.pth')

          return loaded_model
    

    def load_pred_thresh(self):
        with open(f'models/params/{self.name}.pkl', 'rb') as f:
            loaded_ordered_params_dict = pickle.load(f)
            loaded_params_dict = dict(loaded_ordered_params_dict)

        return loaded_params_dict['pred_thresh'] 


    def make_predictions(self):
        
        self.model.eval()

        predictions = self.model(self.pred_tensor)

        predictions_array = predictions.detach().numpy()

        self.display_df['raw_preds'] = predictions_array

        self.display_df = self.display_df[self.display_df['raw_preds'].astype(float) > self.pred_thresh]
        print(f"{len(self.display_df)} AI+EV Bets found!")
        return self.display_df


class MLBRunner():
    
    def __init__(self, name, sport, live_or_pregame):
        self.name = name
        self.sport = sport
        self.live_or_pregame = live_or_pregame

        self.db_manager = DBManager()


    def write_to_cache(self, df, cache_path):
      if not df.index.is_unique:
          df = df.reset_index(drop=True)
      serialized_df = df.to_json()
      redis_client.set(cache_path, serialized_df)
      print(f"df written to: {cache_path}")


    def final_format(self, df):
        
        df['value'] = df['outcome_point'].copy()

        df['other_average_market_odds'] = df['average_market_odds_old_other']

        df['sportsbooks_used'] = df['sportsbooks_used_other'].copy()

        odds_cols = [col for col in df.columns if col.endswith('_1_odds')]

        df.rename(columns={col: col.split('_1_odds')[0] for col in odds_cols}, inplace=True)

        df['sport_title'] = 'baseball_mlb'

        df = map_display_data('sport_title', df)

        df = map_display_data('market_key', df)

        df = map_display_data('wager', df)


        return df


    def write_to_sql(self, df, table_name, engine, if_exists='append'):
        try:
            # Get the table schema
            inspector = inspect(engine)
            columns = inspector.get_columns(table_name)
            
            # Create a dictionary of column names and their SQLAlchemy types
            dtype_dict = {col['name']: col['type'] for col in columns}
            
            # Filter the DataFrame to only include columns that are in the SQL schema
            common_columns = [col for col in df.columns if col in dtype_dict]
            df = df[common_columns]
            
            # Update dtype_dict to only include types for columns that are present in the DataFrame
            dtype_dict = {col: dtype_dict[col] for col in common_columns}
            
            # Function to convert a column to the correct type
            def convert_column(col, sql_type):
                if isinstance(sql_type, types.Integer):
                    return pd.to_numeric(col, errors='coerce').astype('Int64')
                elif isinstance(sql_type, types.Float):
                    return pd.to_numeric(col, errors='coerce')
                elif isinstance(sql_type, types.String):
                    return col.astype(str)
                elif isinstance(sql_type, types.Boolean):
                    return col.astype(bool)
                elif isinstance(sql_type, types.DateTime):
                    return pd.to_datetime(col, errors='coerce')
                else:
                    return col  # If we don't know how to convert, leave it as is

            # Convert each column to the correct type
            for col, sql_type in dtype_dict.items():
                if col in df.columns:
                    df[col] = convert_column(df[col], sql_type)

            # Write to SQL
            df.to_sql(table_name, engine, if_exists=if_exists, index=False, dtype=dtype_dict)
            print(f"Data successfully written to table {table_name}")

        except SQLAlchemyError as e:
            print(f"An error occurred while writing to the database: {str(e)}")
        except Exception as e:
            print(f"An unexpected error occurred: {str(e)}")


    def run(self):

      preprocessor = PreProcessing(self.sport, self.live_or_pregame)

      df = read_cached_df('pos_ev_dash_cache')

      df = df[df['sport_title'] == 'baseball_mlb']

      df = preprocessor.run(df)

      df.to_csv("preprocessed_ai_ev_mlb_df_testing.csv", index=False)

      data_loader_obj = DataLoaderMaker(sport = self.sport, df = df, name = self.name)

      pred_tensor = data_loader_obj.make_data_loaders()

      model = Model(df = df, pred_tensor = pred_tensor, name=self.name)

      df = model.make_predictions()

      df = df.loc[:, ~df.columns.duplicated()]

      df = self.final_format(df)

      self.write_to_cache(df, 'mlb_ai_ev_pregame_cache')

      try:

        self.write_to_sql(table_name='ai_ev_observations', df=df, engine = self.db_manager.get_engine())
        
      except Exception as e:
          print(e)


if __name__ == "__main__":
   
    runner = MLBRunner(name = 'MLB_08_23_2024_model_silu_profit_1000e_0_1e-05lr_Falsewd_1000mb', live_or_pregame='pregame', sport='MLB')

    while True:
      try:
        runner.run()
        time.sleep(300)
      except Exception as e:
         print(e)