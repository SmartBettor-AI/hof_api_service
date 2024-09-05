import pickle
import torch
import pandas as pd
import numpy as np
import requests
from datetime import datetime, timezone
from sklearn.preprocessing import StandardScaler, OneHotEncoder
import os
import warnings
warnings.filterwarnings("ignore")
import time


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
     

def format_time(time_string, output_format="%Y-%m-%d %H:%M:%S"):
    input_formats = [
        "%Y-%m-%dT%H:%M:%SZ",  # Example: 2024-06-06T22:40:00Z
        "%a %b %d %H:%M:%S %Y"  # Example: Thu Jun  6 17:41:16 2024
    ]

    if isinstance(time_string, str):
        for input_format in input_formats:
            try:
                datetime_obj = datetime.strptime(time_string, input_format)
                return datetime_obj.strftime(output_format)
            except ValueError:
                continue
        raise ValueError(f"Time data '{time_string}' does not match any of the known formats.")
    else:
        raise ValueError("Input must be a string")


class pregame_ai_07_17_2024():
    
    
    def __init__(self, name, player_only = False):
        self.model_storage = {}
        self.name = name
        self.player_only = player_only
        self.store_model_info()
        self.display_df = pd.DataFrame()

        self.SHEET_HEADER = ['betonlineag_1_odds',
 'betmgm_1_odds',
 'betrivers_1_odds',
 'betus_1_odds',
 'bovada_1_odds',
 'draftkings_1_odds',
 'fanduel_1_odds',
 'lowvig_1_odds',
 'mybookieag_1_odds',
 'pointsbetus_1_odds',
 'superbook_1_odds',
 'unibet_us_1_odds',
 'williamhill_us_1_odds',
 'wynnbet_1_odds',
 'ballybet_1_odds',
 'betparx_1_odds',
 'espnbet_1_odds',
 'fliff_1_odds',
 'hardrockbet_1_odds',
 'sisportsbook_1_odds',
 'tipico_us_1_odds',
 'windcreek_1_odds',
 'betonlineag_1_points',
 'betmgm_1_points',
 'betrivers_1_points',
 'betus_1_points',
 'bovada_1_points',
 'draftkings_1_points',
 'fanduel_1_points',
 'lowvig_1_points',
 'mybookieag_1_points',
 'pointsbetus_1_points',
 'superbook_1_points',
 'unibet_us_1_points',
 'williamhill_us_1_points',
 'wynnbet_1_points',
 'ballybet_1_points',
 'betparx_1_points',
 'espnbet_1_points',
 'fliff_1_points',
 'hardrockbet_1_points',
 'sisportsbook_1_points',
 'tipico_us_1_points',
 'windcreek_1_points']
        

    def store_model_info(self):
          
          loaded_model = torch.load(f'models/model_objs/{self.name}.pth')

          with open(f'models/encoders/{self.name}.pkl', 'rb') as f:
            loaded_encoder = pickle.load(f)

          with open(f'models/scalers/{self.name}.pkl', 'rb') as f:
            loaded_scaler = pickle.load(f)

          with open(f'models/params/{self.name}.pkl', 'rb') as f:
            loaded_ordered_params_dict = pickle.load(f)
            loaded_params_dict = dict(loaded_ordered_params_dict)

          this_model_dict = {
            'model': loaded_model,
            'encoder': loaded_encoder,
            'scaler': loaded_scaler,
            'params': loaded_params_dict,
            'pred_thresh': loaded_params_dict['pred_thresh']
            }

          if 'quartiles' in loaded_params_dict:
             print('Quartiles found!')
             this_model_dict['quartiles'] = loaded_params_dict['quartiles']
             
        
          self.model_storage['SmartBettorMLBAllMarketsModel'] = this_model_dict


    def process_snapshot(self, df, date):

      data_list = {}

      for bookie in df['bookmakers']:
          
          bookie_column_name = bookie['key'] + "_1_odds"
          if bookie_column_name in self.SHEET_HEADER:
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
                      data_list[full_outcome]['wager'] = wager_key

                      if 'point' in outcome:
                          data_list[full_outcome][bookie['key'] + "_1_points"] = outcome['point']
                      else:
                          data_list[full_outcome][bookie['key'] + "_1_points"] = None
                      
      game_df = pd.DataFrame(data_list)
      game_df = game_df.T.reset_index()
      game_df = game_df.rename(columns={'index': 'outcome'})
      game_df['game_id'] = df['id']
      game_df['commence_time'] = format_time(df['commence_time'])
      game_df['time_pulled'] = format_time(date)
      game_df['home_team'] = df['home_team']
      game_df['away_team'] = df['away_team']
      game_df['sport_title'] = 'baseball_mlb'
      game_df['wager_display'] = ''
      game_df['market_display'] = ''
      game_df['sport_title_display'] = ''

      for column in self.SHEET_HEADER:
          if column not in game_df.columns:
              game_df[column] = np.nan
      
      return game_df


    def get_events_list(self):
      API_KEY = os.environ.get("THE_ODDS_API_KEY")
      SPORT = 'baseball_mlb'

      odds_response = requests.get(
          f'https://api.the-odds-api.com/v4/sports/{SPORT}/events?apiKey={API_KEY}',
      )

      if odds_response.status_code != 200:
          print(f'Failed to get odds: status_code {odds_response.status_code}, response body {odds_response.text}')
      else:
        odds_json = odds_response.json()

        return [game_dict['id'] for game_dict in odds_json]


    def get_mlb_odds(self, event_id):
      
      API_KEY = os.environ.get("THE_ODDS_API_KEY")
      SPORT = 'baseball_mlb'
      REGIONS = 'us,us2'
      MARKETS = 'spreads,h2h,totals,alternate_spreads,alternate_totals,alternate_team_totals,team_totals,h2h_1st_1_innings,h2h_1st_3_innings,h2h_1st_5_innings,h2h_1st_7_innings,h2h_3_way_1st_1_innings,h2h_3_way_1st_3_innings,h2h_3_way_1st_5_innings,h2h_3_way_1st_7_innings,spreads_1st_1_innings,spreads_1st_3_innings,spreads_1st_5_innings,spreads_1st_7_innings,alternate_spreads_1st_1_innings,alternate_spreads_1st_3_innings,alternate_spreads_1st_5_innings,alternate_spreads_1st_7_innings,totals_1st_1_innings,totals_1st_3_innings,totals_1st_5_innings,totals_1st_7_innings,alternate_totals_1st_1_innings,alternate_totals_1st_3_innings,alternate_totals_1st_5_innings,alternate_totals_1st_7_innings,batter_home_runs,batter_first_home_run,batter_hits,batter_total_bases,batter_rbis,batter_runs_scored,batter_hits_runs_rbis,batter_singles,batter_doubles,batter_triples,batter_walks,batter_strikeouts,batter_stolen_bases,pitcher_strikeouts,pitcher_record_a_win,pitcher_hits_allowed,pitcher_walks,pitcher_earned_runs,pitcher_outs' 
    
      ODDS_FORMAT = 'decimal'
      DATE_FORMAT = 'iso'

      odds_response = requests.get(
          f'https://api.the-odds-api.com/v4/sports/{SPORT}/events/{event_id}/odds?apiKey={API_KEY}&regions={REGIONS}&markets={MARKETS}&dateFormat={DATE_FORMAT}&oddsFormat={ODDS_FORMAT}',
      )

      if odds_response.status_code != 200:
          print(f'Failed to get odds: status_code {odds_response.status_code}, response body {odds_response.text}')
      else:

        odds_json = odds_response.json()

        current_utc_time = datetime.now(timezone.utc)

        formatted_utc_time = current_utc_time.strftime("%a %b %d %H:%M:%S %Y")

        # Process this data
        snap = self.process_snapshot(odds_json, formatted_utc_time)

        return snap


    def replace_missing_vals(self, df):
      odds_columns = [col for col in df.columns if 'odds' in col]
      point_columns = [col for col in df.columns if 'point' in col]

      for col in odds_columns:
          df[col] = df[col].replace(np.nan, 0)
          df[col] = df[col].astype('float64')

      for col in point_columns:
          df[col] = df[col].replace(np.nan, 0)
          df[col] = df[col].replace('', 0)
          df[col] = df[col].astype('float64')

      return df


    def make_some_features(self, df):
       
       def make_highest_bettable_odds(df):
          subset_columns = [col for col in df.columns]

          odds_cols = [col for col in subset_columns if '_1_odds' in col]

          # Calculate the maximum odds for each row
          df['highest_bettable_odds'] = df[odds_cols].max(axis=1)

          # df['snapshot_time'] = pd.to_datetime(df['snapshot_time'])

          return df
       
       def make_average_market_odds_old(df):
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

       def make_market_std_dev(df):

        odds_columns = [col for col in df.columns if col.endswith('_odds')]

        subset_df = df[odds_columns].copy()

        subset_df = subset_df.replace(0, np.nan)

        df['odds_std_dev'] = subset_df.std(axis=1)

        subset_df = ''

        return df

       def add_extra_cat_columns(df):
          df['day_or_night'] = df['hour_of_start'].apply(lambda x: 'night' if int(x) > 6 else 'day')
          team_to_conference = {
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
            'San Francisco Giants': 'National League'
            }
          df['home_team_conference'] = df['home_team'].map(team_to_conference)
          df['away_team_conference'] = df['away_team'].map(team_to_conference)


          #Division
          team_to_division = {
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
              'San Francisco Giants': 'NL West'
          }

          df['home_team_division'] = df['home_team'].map(team_to_division)
          df['away_team_division'] = df['away_team'].map(team_to_division)

          return df

       df = make_average_market_odds_old(df)

       df = make_highest_bettable_odds(df)

       df = make_market_std_dev(df)

       df = add_extra_cat_columns(df)

       return df


    def filter_by_params(self, df):
       
       def filter_by_average_market_odds(df):
        df = df[df['average_market_odds_old'] >= self.model_storage['SmartBettorMLBAllMarketsModel']['params']['min_avg_odds']]
        df = df[df['average_market_odds_old'] <= self.model_storage['SmartBettorMLBAllMarketsModel']['params']['max_avg_odds']]
        return df
       
       def filter_by_best_odds(df):
          df = df[df['highest_bettable_odds'] >= self.model_storage['SmartBettorMLBAllMarketsModel']['params']['min_avg_odds']]

          df = df[df['highest_bettable_odds'] <= self.model_storage['SmartBettorMLBAllMarketsModel']['params']['max_avg_odds']]

          return df

       def filter_by_populated_columns(df):
          cols_to_keep = ['home_team','away_team','day_of_week','hour_of_start','home_team_division','away_team_division','home_team_conference','away_team_conference','day_or_night','outcome_name','market_key','draftkings_1_odds', 'williamhill_us_1_odds', 'betmgm_1_odds','draftkings_1_points','fanduel_1_odds', 'betrivers_1_points','fanduel_1_points', 'williamhill_us_1_points', 'betrivers_1_odds','betmgm_1_points', 'average_market_odds_old', 'highest_bettable_odds','odds_std_dev']
         
          return df[cols_to_keep]
    
       def filter_by_ev_thresh(df, MIN_EV = 2):        

        df['ev'] = (1/df['average_market_odds_old'])*(100*df['highest_bettable_odds']-100) - ((1-(1/df['average_market_odds_old'])) * 100)

        df = df[df['ev'] >= MIN_EV]

        df = df.drop(columns=['ev'], axis='columns')

        return df


       df = filter_by_average_market_odds(df)

       df = filter_by_best_odds(df)

       df = filter_by_ev_thresh(df)

       self.display_df = df.copy()

       df = filter_by_populated_columns(df)

       return df


    def preprocess(self, df):

      if self.player_only:
        mask = df['market_key'].str.contains('pitcher|batter')
        df = df[mask]

      df['commence_time'] = pd.to_datetime(df['commence_time'])
      df['day_of_week'] = df['commence_time'].dt.strftime('%a')
      df['hour_of_start'] = df["commence_time"].dt.hour.astype(str)
      df['snapshot_time'] = df['commence_time']

      self.stacked_df_missing_vals = self.replace_missing_vals(df)

      self.stacked_df_features_added = self.make_some_features(self.stacked_df_missing_vals)

      self.filtered_df = self.filter_by_params(self.stacked_df_features_added)

      return 
    

    def format_for_nn(self):

       self.categorical_columns = ['home_team', 'away_team', 'day_of_week', 'hour_of_start','home_team_division', 'away_team_division', 'home_team_conference', 'away_team_conference', 'day_or_night', 'outcome_name', 'market_key']

       self.numerical_columns = ['draftkings_1_odds', 'williamhill_us_1_odds', 'betmgm_1_odds','draftkings_1_points', 'fanduel_1_odds', 'betrivers_1_points', 'fanduel_1_points', 'williamhill_us_1_points','betrivers_1_odds','betmgm_1_points','average_market_odds_old','highest_bettable_odds','odds_std_dev']
       
       def standardize_numerical_values(df):

        # Select the subset 
        df_numerical = df[self.numerical_columns]

        # Create an instance of StandardScaler and fit it on the training data
        scaler = self.model_storage['SmartBettorMLBAllMarketsModel']['scaler']

        scaled_df = scaler.transform(df_numerical)
        
        return scaled_df
      
      # Make sure this is similiar as well to the pregame maker 
       def encode_categorical_variables(df):

        encoded = np.empty((len(df), 0))

        encoded_column_names = []  

        encoder_obj = self.model_storage['SmartBettorMLBAllMarketsModel']['encoder']

        for col in self.categorical_columns:

            col_encoder = encoder_obj[col]

            col_data = df[[col]].astype(str)

            encoded_columns = col_encoder.transform(col_data)

            # Concatenate encoded columns along axis 1 (columns) within each dataset
            encoded = np.hstack((encoded, encoded_columns))

        # Generate column names based on the encoding
        for i in range(encoded.shape[1]):
            col_name = f"encoded_{i}"
            encoded_column_names.append(col_name)

        return encoded
      
       self.scaled_arr = standardize_numerical_values(self.filtered_df)

       self.coded_arr = encode_categorical_variables(self.filtered_df)

       self.final_data_for_model = np.hstack((self.scaled_arr, self.coded_arr))

       self.final_data_for_model = self.final_data_for_model.astype(np.float32)

       return


    # Go through this logic to make sure this makes sense, also add bettable books 
    def make_live_dash_data(self):

      print(f'{self.name} AI running')

      events = self.get_events_list()

      market_odds_df = pd.DataFrame()
      for event in events:
        market_odds_df = pd.concat([market_odds_df, self.get_mlb_odds(event)], ignore_index=True)

      # Makes self.filtered_df
      self.preprocess(market_odds_df)
      
      if not self.filtered_df.empty:

        for strategy_name, strategy_dict in self.model_storage.items():
          
          # working thru this now 
          self.format_for_nn()

          input_tensor = torch.tensor(self.final_data_for_model, dtype=torch.float32)

          strategy_dict['model'].eval()

          predictions = strategy_dict['model'](input_tensor)

          predictions_array = predictions.detach().numpy()

          self.display_df['raw_preds'] = predictions_array

          mask = predictions_array > strategy_dict['pred_thresh']

          filtered_df = self.display_df[mask]

          if not filtered_df.empty:

            proper_columns = ['fanduel_1_odds', 'williamhill_us_1_points', 'betrivers_1_points', 'fanduel_1_points', 'draftkings_1_points', 'williamhill_us_1_odds', 'betrivers_1_odds', 'betmgm_1_odds', 'betmgm_1_points', 'draftkings_1_odds']

            if self.name == 'mlb_player_only_model':
              filtered_df = filtered_df[(filtered_df[proper_columns] != 0).sum(axis=1) > 3]
            else:
              # filtered_df = filtered_df[(filtered_df[proper_columns] != 0).sum(axis=1) > 5]
              pass

            filtered_df = map_display_data('sport_title', filtered_df)

            filtered_df = map_display_data('market_key', filtered_df)

            filtered_df = map_display_data('wager', filtered_df)

            filtered_df['model_name'] = self.name

            if 'quartiles' in self.model_storage['SmartBettorMLBAllMarketsModel']:
               filtered_df['quartile_rank'] = np.digitize(filtered_df['raw_preds'], self.model_storage['SmartBettorMLBAllMarketsModel']['quartiles'])

            # existing_df = pd.read_csv( f'ai_model_output/{self.name}.csv')

            # result_df = pd.concat([existing_df, filtered_df], ignore_index=True)
            
            if 'quartiles' in self.model_storage['SmartBettorMLBAllMarketsModel']:
              result_df['confidence'] = result_df['quartile_rank'].fillna(1).astype(int).map({0: 'Fair', 1: 'High', 2: 'Very High', 3: 'Extreme'})

            filtered_df.to_csv( f'ai_model_output/{self.name}.csv', 
               mode = 'w', 
               index = False
            )

            print(f"{len(filtered_df)} AI MLB bets found" )

          elif filtered_df.empty:
             pass
      return
    