import pandas as pd
import requests
from datetime import datetime, timedelta, time
import numpy as np
import redis
import sqlite3


redis_client = redis.Redis(host='localhost', port=6379, db=0)

SHEET_HEADER = ['game_id', 'commence_time', 'time_pulled', 'team_1', 'team_2','barstool_1_odds', 'barstool_1_time', 'barstool_2_odds', 'barstool_2_time',      'betclic_1_odds', 'betclic_1_time', 'betclic_2_odds', 'betclic_2_time', 'betfair_1_odds', 'betfair_1_time', 'betfair_2_odds', 'betfair_2_time', 'betfred_1_odds', 'betfred_1_time', 'betfred_2_odds', 'betfred_2_time', 'betmgm_1_odds', 'betmgm_1_time', 'betmgm_2_odds', 'betmgm_2_time', 'betonlineag_1_odds', 'betonlineag_1_time', 'betonlineag_2_odds', 'betonlineag_2_time', 'betrivers_1_odds', 'betrivers_1_time', 'betrivers_2_odds', 'betrivers_2_time', 'betus_1_odds', 'betus_1_time', 'betus_2_odds', 'betus_2_time', 'betway_1_odds', 'betway_1_time', 'betway_2_odds', 'betway_2_time', 'bovada_1_odds', 'bovada_1_time', 'bovada_2_odds', 'bovada_2_time', 'casumo_1_odds', 'casumo_1_time', 'casumo_2_odds', 'casumo_2_time', 'circasports_1_odds', 'circasports_1_time', 'circasports_2_odds', 'circasports_2_time', 'coral_1_odds', 'coral_1_time', 'coral_2_odds', 'coral_2_time', 'draftkings_1_odds', 'draftkings_1_time', 'draftkings_2_odds', 'draftkings_2_time', 'fanduel_1_odds', 'fanduel_1_time', 'fanduel_2_odds', 'fanduel_2_time', 'foxbet_1_odds', 'foxbet_1_time', 'foxbet_2_odds', 'foxbet_2_time', 'gtbets_1_odds', 'gtbets_1_time', 'gtbets_2_odds', 'gtbets_2_time', 'ladbrokes_1_odds', 'ladbrokes_1_time', 'ladbrokes_2_odds', 'ladbrokes_2_time', 'lowvig_1_odds', 'lowvig_1_time', 'lowvig_2_odds', 'lowvig_2_time', 'marathonbet_1_odds', 'marathonbet_1_time', 'marathonbet_2_odds', 'marathonbet_2_time', 'matchbook_1_odds', 'matchbook_1_time', 'matchbook_2_odds', 'matchbook_2_time', 'mrgreen_1_odds', 'mrgreen_1_time', 'mrgreen_2_odds', 'mrgreen_2_time', 'mybookieag_1_odds', 'mybookieag_1_time', 'mybookieag_2_odds', 'mybookieag_2_time', 'nordicbet_1_odds', 'nordicbet_1_time', 'nordicbet_2_odds', 'nordicbet_2_time', 'onexbet_1_odds', 'onexbet_1_time', 'onexbet_2_odds', 'onexbet_2_time', 'paddypower_1_odds', 'paddypower_1_time', 'paddypower_2_odds', 'paddypower_2_time', 'pinnacle_1_odds', 'pinnacle_1_time', 'pinnacle_2_odds', 'pinnacle_2_time', 'pointsbetus_1_odds', 'pointsbetus_1_time', 'pointsbetus_2_odds', 'pointsbetus_2_time', 'sport888_1_odds', 'sport888_1_time', 'sport888_2_odds', 'sport888_2_time', 'sugarhouse_1_odds', 'sugarhouse_1_time', 'sugarhouse_2_odds', 'sugarhouse_2_time', 'superbook_1_odds', 'superbook_1_time', 'superbook_2_odds', 'superbook_2_time', 'twinspires_1_odds', 'twinspires_1_time', 'twinspires_2_odds', 'twinspires_2_time', 'unibet_1_odds', 'unibet_1_time', 'unibet_2_odds', 'unibet_2_time', 'unibet_eu_1_odds', 'unibet_eu_1_time', 'unibet_eu_2_odds', 'unibet_eu_2_time', 'unibet_uk_1_odds', 'unibet_uk_1_time', 'unibet_uk_2_odds', 'unibet_uk_2_time', 'unibet_us_1_odds', 'unibet_us_1_time', 'unibet_us_2_odds', 'unibet_us_2_time', 'williamhill_1_odds', 'williamhill_1_time', 'williamhill_2_odds', 'williamhill_2_time', 'williamhill_us_1_odds', 'williamhill_us_1_time', 'williamhill_us_2_odds', 'williamhill_us_2_time', 'wynnbet_1_odds', 'wynnbet_1_time', 'wynnbet_2_odds', 'wynnbet_2_time']
import os

def read_cached_df(path):
    serialized_df = redis_client.get(path)
    if serialized_df:
      serialized_df = serialized_df.decode('utf-8')
      cached_df = pd.read_json(serialized_df)
      return cached_df
    else:
      raise FileNotFoundError(f"No cached DataFrame found at path: {path}")




def map_commence_time_game_id():
        df = pd.read_parquet('mlb_data/2023_data_for_val.parquet')
        df['commence_time'] = pd.to_datetime(df['commence_time'])

        ret_dict = df.set_index('game_id')['commence_time'].to_dict()

        for key, value in ret_dict.items():
            ret_dict[key] = value.date()
        return ret_dict

def format_time(time_string, input_format="%Y-%m-%dT%H:%M:%SZ", output_format="%Y-%m-%d %H:%M:%S"):
    if isinstance(time_string, str):
        datetime_obj = datetime.strptime(time_string, input_format)
    else:
        datetime_obj = time_string

    output_time_string = datetime_obj.strftime(output_format)
    return output_time_string


def get_odds():
    API_KEY = os.environ.get("THE_ODDS_API_KEY")

    SPORT = 'baseball_mlb'

    REGIONS = 'us,eu,uk'

    MARKETS = 'h2h' 

    ODDS_FORMAT = 'decimal'

    DATE_FORMAT = 'iso'

    odds_response = requests.get(
        f'https://api.the-odds-api.com/v4/sports/{SPORT}/odds/',
        params={
            'api_key': API_KEY,
            'regions': REGIONS,
            'markets': MARKETS,
            'oddsFormat': ODDS_FORMAT,
        }
    )

    if odds_response.status_code != 200:
        print(f'Failed to get odds: status_code {odds_response.status_code}, response body {odds_response.text}')
    else:
        odds_json = odds_response.json()

        # Make a dataframe from this pull
        df = pd.DataFrame.from_dict(odds_json)

        # Process this data
        snap = make_snapshot(odds_json)

        return snap

def make_snapshot(df):

    snapshot_df = pd.DataFrame(columns=SHEET_HEADER)

    current_time_utc = datetime.utcnow()

    date = current_time_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    my_dict = {value: '' for value in SHEET_HEADER}


    for game in df:
        # Information about the game
        my_dict['game_id'] = game['id']
        my_dict['commence_time'] = format_time(game['commence_time'])
        my_dict['time_pulled'] = format_time(date)

        # Compiles each bookmakers lines into a dictionary and then appends that row to a df
        for bookie in game['bookmakers']:
            # Get team name
            my_dict['team_1'] = bookie['markets'][0]['outcomes'][0]['name']
            my_dict['team_2'] = bookie['markets'][0]['outcomes'][1]['name']

            # Find the appropriate column
            if f'{bookie["key"]}_1_odds' in my_dict and f'{bookie["key"]}_2_odds' in my_dict:

                my_dict[bookie['key'] + "_1_odds"] = bookie['markets'][0]['outcomes'][0]['price']
                my_dict[bookie['key'] + "_1_time"] = format_time(bookie['last_update'])

                my_dict[bookie['key'] + "_2_odds"] = bookie['markets'][0]['outcomes'][1]['price']
                my_dict[bookie['key'] + "_2_time"] = format_time(bookie['last_update'])

        snapshot_df = snapshot_df.append(my_dict, ignore_index=True)

        my_dict = {value: '' for value in SHEET_HEADER}

    return snapshot_df

def preprocess(df):
    df = convert_times_to_mst(df)

    df, extra_info_df = make_my_game_id(df)

    df = map_between_sheets(df, extra_info_df)

    return df

def convert_times_to_mst(df):
    # Actually converts it to MSt when we're in daylight savings time, but as long as we're not in GMT we're chilling
    time_columns = [col for col in df.columns if 'time' in col]
    
    df[time_columns] = df[time_columns].apply(lambda x: pd.to_datetime(x))
    df[time_columns] = df[time_columns]- pd.Timedelta(hours=7)

    return df 

def make_my_game_id(df):
    #extra_info_df = pd.read_csv('mlb_data/mlb_extra_info.csv')
    conn = sqlite3.connect('smartbetter.db')
    extra_info_df = pd.read_sql('SELECT * FROM mlb_extra_info', conn)
    conn.close()   # Close the connection

    extra_info_df['home_team_final'] = extra_info_df[['home_team', 'away_team']].min(axis=1)
    extra_info_df['away_team_final'] = extra_info_df[['home_team', 'away_team']].max(axis=1)



    df['date'] = df['commence_time'].copy().dt.strftime('%Y%m%d')
    extra_info_df['date']= extra_info_df['date'].astype(str)

    # COME BACK HERE IF SPOT TST DOESN'T WORK
    df['my_id'] = df['team_1'] + df['team_2'] + df['date']

    extra_info_df['my_id'] = extra_info_df['home_team_final'] + extra_info_df['away_team_final'] + extra_info_df['date']

    return df, extra_info_df

def map_between_sheets(df, extra_info_df):

    mapping_dict = map_my_id_to_game_id(df)

    extra_info_df['number_of_game_today'] = extra_info_df['number_of_game_today'].astype(str)

    extra_dict = map_my_id_to_double_header_vals(extra_info_df)

    id_commence_dict = map_game_id_to_commence_time(df)

    new_dict, id_commence_dict = fix_mapping_dict(mapping_dict, id_commence_dict)

    extra_info_df = make_my_id_game(extra_info_df)


    my_id_game = extra_info_df['my_id_game'].tolist()

    new_list = []
    for key, value in new_dict.items():
        if key in my_id_game:
            new_list.append(key)

    df['my_game_id_final'] = df['game_id'].apply(lambda x: next((k for k, v in new_dict.items() if v == x), None))

    df = finish_combination(df, extra_info_df)

    return df

def map_my_id_to_game_id(df):
    mapping_dict = {}
    for key, value in zip(df['my_id'], df['game_id']):
        if key in mapping_dict:
            mapping_dict[key].append(value)
            mapping_dict[key] = list(set(mapping_dict[key]))
        else:
            mapping_dict[key] = [value]  

    return mapping_dict

def map_my_id_to_double_header_vals(extra_info_df):
    # Make a dict that maps extra my_id to the extra game_value (double header or not)
    extra_dict = {}
    for key, value in zip(extra_info_df['my_id'], extra_info_df['number_of_game_today']):
        if key in extra_dict:
            extra_dict[key].append(value)
            extra_dict[key] = list(extra_dict[key])
        else:
            extra_dict[key] = [value]

    return extra_dict

def map_game_id_to_commence_time(df):
    id_commence_dict = {key: value for key, value in zip(df['game_id'], df['commence_time'])}
    return id_commence_dict

def fix_mapping_dict(mapping_dict, id_commence_dict):
    new_dict = {}
    for key, values in mapping_dict.items():
        if len(values) == 1:
            new_dict[str(str(key) + '_0')] = values[0]
        elif len(values) == 2:
            if id_commence_dict[values[0]] < id_commence_dict[values[1]]:
                new_dict[str(str(key) + '_1')] = values[0]
                new_dict[str(str(key) + '_2')] = values[1]
            elif id_commence_dict[values[0]] > id_commence_dict[values[1]]:
                new_dict[str(str(key) + '_2')] = values[0]
                new_dict[str(str(key) + '_1')] = values[1]

    return new_dict, id_commence_dict

def make_my_id_game(extra):
    extra['my_id_game'] = extra['home_team_final'] + extra['away_team_final'] + extra['date'] + '_' + extra['number_of_game_today']

    return extra

def finish_combination(df, extra):

    df['number_of_game_today'] = df['my_game_id_final'].map(extra.set_index('my_id_game')['number_of_game_today'])

    df['day_of_week'] = df['my_game_id_final'].map(extra.set_index('my_id_game')['day_of_week'])

    df['away_team'] = df['my_game_id_final'].map(extra.set_index('my_id_game')['away_team'])
    df['away_team_league'] = df['my_game_id_final'].map(extra.set_index('my_id_game')['away_team_league'])
    df['away_team_game_number'] = df['my_game_id_final'].map(extra.set_index('my_id_game')['away_team_game_number'])

    df['home_team'] = df['my_game_id_final'].map(extra.set_index('my_id_game')['home_team'])
    df['home_team_league'] = df['my_game_id_final'].map(extra.set_index('my_id_game')['home_team_league'])
    df['home_team_game_number'] = df['my_game_id_final'].map(extra.set_index('my_id_game')['home_team_game_number'])

    df['day_night'] = df['my_game_id_final'].map(extra.set_index('my_id_game')['day_night'])
    df['park_id'] = df['my_game_id_final'].map(extra.set_index('my_id_game')['park_id'])

    return df

def make_stacked_df(pull_df):

    stacked_df = stack_df(pull_df)

    return stacked_df

def stack_df(df):
    # Read the column names 
    all_columns = df.columns
    columns = all_columns[5:]
    info_columns = ['game_id', 'commence_time', 'time_pulled', 'home_team', 'away_team', 'team_1', 'team_2']

    odds_columns = [x for x in columns if x.endswith('_odds')]

    time_columns = [x for x in columns if x.endswith('_time')]

    categorical_columns = ['day_of_week', 'away_team_league', 'home_team_league', 'day_night', 'park_id']

    numerical_columns = ['number_of_game_today', 'away_team_game_number', 'home_team_game_number',]

    for col in odds_columns:
        df[col] = df[col].replace(np.nan, 0)
        df[col] = df[col].replace('', 0)
        df[col] = df[col].astype('float64')
    # Replace all the missing time values with Jan 1, 1970
    for col in time_columns:
        df[col] = df[col].replace(np.nan, '1/1/1970 00:00:00')
        df[col] = df[col].replace('', '1/1/1970 00:00:00')

    for col in time_columns:
        df[col] = pd.to_datetime(df[col])
    df['snapshot_time_taken'] = df[time_columns].apply(lambda x: max(x), axis=1)
    # We need to use that column as the time_pulled variable rather than the actual time pulled 
    df['snapshot_time_taken'] = pd.to_datetime(df['snapshot_time_taken'])
    df['commence_time'] = pd.to_datetime(df['commence_time'])

    df['minutes_since_commence'] = (df['snapshot_time_taken'] - df['commence_time']).dt.total_seconds()/60

    df['hour_of_start'] = df['commence_time'].dt.hour

    # TODO: Idk why this is here
    new_data = df[df['minutes_since_commence'] >= -10000000]

    # Select the first subset:
    cols_with_one = [col for col in new_data.columns if '_1' in col]
    cols_with_two = [col for col in new_data.columns if '_2' in col]
    extra_cols = ['game_id', 'commence_time', 'time_pulled', 'home_team', 'away_team', 'number_of_game_today', 'day_of_week', 'away_team_league', 'away_team_game_number', 'home_team_league', 'home_team_game_number', 'day_night', 'park_id', 'minutes_since_commence', 'snapshot_time_taken', 'hour_of_start']

    for each in extra_cols:
        cols_with_one.append(each)
        cols_with_two.append(each)

    df1 = new_data[cols_with_one]
    df2 = new_data[cols_with_two]

    # # Get list of column names from df1
    df1_cols = df1.columns.tolist()

    # # Create dictionary to map column names in df2 to column names in df1
    col_map = {col: df1_cols[i] for i, col in enumerate(df2.columns)}

    # # Rename columns in df2 using dictionary
    df2 = df2.rename(columns=col_map)

    # Concatenate the subsets vertically
    df_stacked = pd.concat([df1, df2], axis=0, ignore_index=True)

    # Reset the index of the new DataFrame
    df_stacked = df_stacked.reset_index(drop=True)
    df_stacked['opponent'] = np.where(df_stacked['team_1'] == df_stacked['home_team'], df_stacked['away_team'], df_stacked['home_team'])

    df_stacked['this_team_league'] = np.where(df_stacked['team_1'] == df_stacked['home_team'], df_stacked['home_team_league'], df_stacked['away_team_league'])

    df_stacked['opponent_league'] = np.where(df_stacked['team_1'] == df_stacked['home_team'], df_stacked['away_team_league'], df_stacked['home_team_league'])

    df_stacked['this_team_game_of_season'] = np.where(df_stacked['team_1'] == df_stacked['home_team'], df_stacked['home_team_game_number'], df_stacked['away_team_game_number'])

    df_stacked['opponent_game_of_season'] = np.where(df_stacked['team_1'] == df_stacked['home_team'], df_stacked['away_team_game_number'], df_stacked['home_team_game_number'])

    df_stacked['home_away'] = np.where(df_stacked['team_1'] == df_stacked['home_team'], 1, 0)

    df_stacked['snapshot_time'] = df_stacked['snapshot_time_taken'].dt.time

    # cols_to_drop=['commence_time', 'time_pulled', 'home_team', 'away_team', 'away_team_league', 'away_team_game_number','home_team_league', 'home_team_game_number', 'snapshot_time_taken']

    # result = df_stacked.drop(columns=cols_to_drop)

    # Re-order the columns to match the order in the data_collector.py read-in file in OddsNet
    result = df_stacked[['barstool_1_odds', 'barstool_1_time', 'betclic_1_odds', 'betclic_1_time', 'betfair_1_odds', 'betfair_1_time', 'betfred_1_odds', 'betfred_1_time', 'betmgm_1_odds', 'betmgm_1_time', 'betonlineag_1_odds', 'betonlineag_1_time', 'betrivers_1_odds', 'betrivers_1_time', 'betus_1_odds', 'betus_1_time', 'betway_1_odds', 'betway_1_time', 'bovada_1_odds', 'bovada_1_time', 'casumo_1_odds', 'casumo_1_time', 'circasports_1_odds', 'circasports_1_time', 'coral_1_odds', 'coral_1_time', 'draftkings_1_odds', 'draftkings_1_time', 'fanduel_1_odds', 'fanduel_1_time', 'foxbet_1_odds', 'foxbet_1_time', 'gtbets_1_odds', 'gtbets_1_time', 'ladbrokes_1_odds', 'ladbrokes_1_time', 'lowvig_1_odds', 'lowvig_1_time', 'marathonbet_1_odds', 'marathonbet_1_time', 'matchbook_1_odds', 'matchbook_1_time', 'mrgreen_1_odds', 'mrgreen_1_time', 'mybookieag_1_odds', 'mybookieag_1_time', 'nordicbet_1_odds', 'nordicbet_1_time', 'onexbet_1_odds', 'onexbet_1_time', 'paddypower_1_odds', 'paddypower_1_time', 'pinnacle_1_odds', 'pinnacle_1_time', 'pointsbetus_1_odds', 'pointsbetus_1_time', 'sport888_1_odds', 'sport888_1_time', 'sugarhouse_1_odds', 'sugarhouse_1_time', 'superbook_1_odds', 'superbook_1_time', 'twinspires_1_odds', 'twinspires_1_time', 'unibet_1_odds', 'unibet_1_time', 'unibet_eu_1_odds', 'unibet_eu_1_time', 'unibet_uk_1_odds', 'unibet_uk_1_time', 'unibet_us_1_odds', 'unibet_us_1_time', 'williamhill_1_odds', 'williamhill_1_time', 'williamhill_us_1_odds', 'williamhill_us_1_time', 'wynnbet_1_odds', 'wynnbet_1_time', 'team_1', 'game_id', 'number_of_game_today', 'day_of_week', 'day_night', 'park_id', 'minutes_since_commence', 'hour_of_start', 'opponent', 'this_team_league', 'opponent_league', 'this_team_game_of_season', 'opponent_game_of_season', 'home_away', 'snapshot_time']]

    return result


def american_to_decimal(american_odds):
    american_odds_int = int(american_odds)
    if american_odds_int >= 0:
        decimal = (american_odds_int/100) + 1
        return decimal

    elif american_odds_int < 0:

        decimal = (100/abs(american_odds_int)) + 1
        return decimal
    
def decimal_to_american(decimal_odds):

    decimal_odds_int = float(decimal_odds)

    if decimal_odds_int >= 2:
        american_odds = (decimal_odds_int-1) * 100
        return int(american_odds)

    elif decimal_odds_int < 2:
        american_odds = -100 / (decimal_odds_int-1)
        return int(american_odds)




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
     