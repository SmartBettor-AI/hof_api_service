from flask import Blueprint, jsonify, request, current_app
api = Blueprint('main', __name__)

from ..util import read_cached_df
import pandas as pd
import os
from datetime import datetime, timezone, timedelta
import json
import time
import numpy as np
import requests
import math
import logging
import redis
redis_client = redis.Redis(host='redis-13193.c309.us-east-2-1.ec2.redns.redis-cloud.com', port=13193, password="GCNR3ozzjveUlQniIrCgmJO9UL7Ek0oo", db=0)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def minutes_seconds(row):
          seconds = int(float(row['time_difference_seconds']))
          if seconds < 60:
            row['time_difference_formatted'] = f'{seconds} sec'

          elif seconds >= 60 and seconds < 3600:
            minutes = math.floor(seconds / 60)
            new_seconds = (seconds % 60)
            row['time_difference_formatted'] = f'{minutes} min {new_seconds} sec'
             
          else:
            hours = math.floor(seconds / 3600)
            seconds_after_hour = seconds % 3600
            new_minutes = math.floor(seconds_after_hour / 60)
            new_seconds = seconds_after_hour % 60
            row['time_difference_formatted'] = f'{hours} hours {new_minutes} min {new_seconds} sec'
          
          return row


@api.route('/api/pregame')
def pregame():
    
    df = read_cached_df('ai_dash_cache')

    response = historical_results()

    df = df[df['completed'].astype(int) == 0]

    df = df[df['model_name'] == 'mlb_07_18_2024_model']

    # df = df[df['average_market_odds_old'].astype(float) <= 2.5]

    # df = df[df['average_market_odds_old'].astype(float) >= 1.665]

    df = df[df['market_key'].isin(['spreads', 'h2h', 'team_totals'])]

    datetime_cols = df.select_dtypes(include=['datetime']).columns

    df[datetime_cols] = df[datetime_cols].astype(object).where(df[datetime_cols].notnull(), None)

    df['commence_time'] = pd.to_datetime(df['commence_time'])

    df['time_pulled'] = pd.to_datetime(df['time_pulled'])

    df['minutes_till_commence'] = (df['commence_time'] - df['time_pulled']).dt.total_seconds() / 60

    df = df[df['minutes_till_commence'] < 180]
    df = df[df['minutes_till_commence'] > 0]

    df['rec_bet_size'] = (1 / df['highest_bettable_odds'] * 10)

    current_date_gmt = datetime.now()

    # df = df[df['time_pulled'] < df['commence_time']]

    df = df.fillna('')

    df['average_market_odds_old_rounded'] = df['highest_bettable_odds'].round(2)

    df['time_difference_seconds'] = (current_date_gmt - df['time_pulled']).dt.total_seconds()

    df['game_date']= (df['commence_time'] - pd.Timedelta(hours=6)).dt.strftime('%A, %B %d, %Y')

    df = df.apply(minutes_seconds, axis=1)

    df = df.fillna('')
    
    response['quality'] = round(response['observed_win_rate'] * 100, 1)

    df = df.merge(response, how='left', on='market_key')

    df['expected_win_rate']= 1/df['highest_bettable_odds']

    df = df.fillna('')

    df = df.groupby('outcome').last().reset_index()

    data_json = df.to_dict(orient='records')

    return jsonify(data_json)



@api.route('/api/winnable')
def winnable():
    
    df = read_cached_df('ai_dash_cache')

    response = requests.get('http://localhost:5000/api/historical_results/win_rates')

    df = df[df['model_name'] == 'mlb_06_10_2024_model_v2']

    df = df[df['completed'].astype(int) == 0]

    # df = df[(df['average_market_odds_old'] < 2.5) & (df['average_market_odds_old'] > 1.66)]

    datetime_cols = df.select_dtypes(include=['datetime']).columns

    df[datetime_cols] = df[datetime_cols].astype(object).where(df[datetime_cols].notnull(), None)

    df['commence_time'] = pd.to_datetime(df['commence_time']).dt.tz_localize('UTC')

    df['time_pulled'] = pd.to_datetime(df['time_pulled']).dt.tz_localize('UTC')

    current_date_gmt = datetime.now(timezone.utc)

    df = df[df['commence_time'] > current_date_gmt]

    df = df[df['time_pulled'] < df['commence_time']]

    df = df.fillna('')

    df = df[df['market_display'] != '']

    df['average_market_odds_old_rounded'] = df['average_market_odds_old'].round(2)

    df['time_difference_seconds'] = (current_date_gmt - df['time_pulled']).dt.total_seconds()

    df['game_date']= (df['commence_time'] - pd.Timedelta(hours=6)).dt.strftime('%A, %B %d, %Y')

    df = df.apply(minutes_seconds, axis=1)

    df = df.fillna('')

    if response.status_code == 200:
        other_route_data = response.json()
        
        win_rates = pd.DataFrame(other_route_data)

        df = df.merge(win_rates, how='left', left_on='average_market_odds_old_rounded', right_on='average_market_odds_old_rounded')

        df = df.fillna('')

    df = df.groupby('outcome').last().reset_index()

    data_json = df.to_dict(orient='records')

    return jsonify(data_json)


def historical_results():
    
    df = pd.read_csv('historical_performance/raw/mlb_07_18_2024_model.csv')

    df = df[df['realized_w_l'] != "PUSH"]

    unique_markets = sorted(df['market_key'].unique())

    json_array = []

    for val in unique_markets:

        odds_df = df[df['market_key'] >= val]

        obs = {
             'market_key': val,
             'observed_win_rate': len(odds_df[odds_df['realized_w_l'].astype(int) == 1]) / len(odds_df),
        }

        json_array.append(obs)

    return pd.DataFrame(json_array)


@api.route('/api/performance')
def performance():
    model_name = request.args.get('model')
    date_range = request.args.get('dateRange')
    kelley = float(request.args.get('kelley'))
    starting_bankroll = float(request.args.get('bankroll'))
    min_odds = request.args.get('min_odds')
    max_odds = request.args.get('max_odds')
    markets = request.args.get('markets')

    print(kelley)




    if not model_name:
        logger.error("Model name not provided")
        return jsonify({'error': 'Model name not provided'}), 400
    
    if not date_range:
        logger.error("Date range not provided")
        return jsonify({'error': 'Date range not provided'}), 400

    engine = current_app.db_manager.get_engine()

    # Define the SQL query
    query = f"""
    SELECT *
    FROM (
        SELECT time_pulled, realized_w_l, average_market_odds_old, commence_time, highest_bettable_odds, 
               market_key, market_display, commence_date,
               ROW_NUMBER() OVER (PARTITION BY outcome, commence_date ORDER BY time_pulled DESC) AS rn
        FROM ai_ev_graded
    ) AS subquery
    WHERE rn = 1
    """

    # Execute the query and load the data into a DataFrame
    df = pd.read_sql(query, engine)

    if 'commence_date' not in df.columns:
        logger.error(f"'commence_date' column not found in sql")
        return jsonify({'error': 'commence_date column not found in CSV file'}), 500
    today = datetime.now(timezone.utc) - timedelta(hours=6)
    today = today.replace(tzinfo=None)
    today = today.replace(hour=0, minute=0, second=0, microsecond=0)

    date_range_map = {
        '1d': timedelta(days=1),
        '7d': timedelta(days=7),
        '30d': timedelta(days=30),
        '6mo': timedelta(days=30*6),
        '12mo': timedelta(days=30*12)
    }
    if date_range not in date_range_map:
        logger.error(f"Unsupported time range: {date_range}")

        return jsonify({'error': f'Unsupported time range: {date_range}'}), 400
    start_date = today - date_range_map[date_range]
    if min_odds:
        df = df[df['average_market_odds_old'].astype(float) >= float(min_odds)]
    if max_odds:
        df = df[df['average_market_odds_old'].astype(float) <= float(max_odds)]
    try:
      df['commence_date'] = pd.to_datetime(df['commence_date'], format="%Y_%m_%d")

      df = df[df['commence_date'] >= start_date]
      if markets:
        markets = markets.split(',')
        df = df[df['market_display'].isin(markets)]
      df.to_csv("performance_data_test_1.csv", index=False)
     

      df['decimal_prob'] = 1/df['average_market_odds_old']

      df['kelley_perc'] = ((df['highest_bettable_odds'] - 1) * df['decimal_prob'] - (1-df['decimal_prob'])) / (df['highest_bettable_odds']-1) * kelley

      df['kelley_perc'] = np.where(df['kelley_perc'] < 0, 0, df['kelley_perc'])

      df['bet_amount'] = df['kelley_perc'] * starting_bankroll

      df = df[df['realized_w_l'] != 'PUSH']

      df['bet_result'] = np.where(df['realized_w_l'].astype(float) > 0, df['bet_amount'] * df['highest_bettable_odds'] - df['bet_amount'], df['bet_amount'] * -1)

      df.to_csv("performance_data_test_2.csv", index=False)


      grouped = df.groupby('commence_date').agg({'bet_result': ['sum', 'size']}).reset_index()
      grouped.columns = ['_'.join(col).strip() for col in grouped.columns.values]
      #                        date        total units won/lost   total bets
      grouped.columns = ['commence_date', 'total_bet_result', 'bet_result_count']

      grouped['win_count'] = len(df[df['realized_w_l'].astype(str) == '1'])
      grouped['loss_count'] = len(df[df['realized_w_l'].astype(str) == '0'])
      grouped['push_count'] = len(df[df['realized_w_l'].astype(str) == 'PUSH'])
      # what we want here is to have a daily starting bankroll and daily ending bankroll
      grouped['daily_starting_bankroll'] = starting_bankroll
      grouped['total_bet_result_running'] = grouped['total_bet_result'].cumsum()
      grouped['daily_ending_bankroll'] = grouped['daily_starting_bankroll'] +  grouped['total_bet_result_running']
      grouped['shifted_col1'] = grouped['total_bet_result'].shift(1, fill_value=0).cumsum()
      grouped['daily_starting_bankroll'] = grouped['daily_starting_bankroll'] +  grouped['shifted_col1']
      grouped['Bankroll'] = grouped['total_bet_result_running'] + starting_bankroll
      grouped['commence_date'] = grouped['commence_date'].apply(lambda x: x.isoformat())
      grouped['total_bet_amount'] = df['bet_amount'].sum() 



    except Exception as e:
        logger.info(f"Error occured while grouping by day, {e}")
        return jsonify({'error': 'Error collecting data by day.'}), 500


    if df.empty:
        logger.info(f"No data available for the specified date range: {date_range}")
        return jsonify({'message': 'No data available for the specified date range'})

    try:
        data_json = grouped.to_dict(orient='records')
        logger.info(f"Successfully converted data to JSON. Records: {len(data_json)}")
    except Exception as e:
        logger.error(f"Error converting data to JSON: {str(e)}")
        return jsonify({'error': f'Error converting data to JSON: {str(e)}'}), 500

    return jsonify(data_json)


@api.route('/api/filter_model_performance_by_odds')
def filter_model_performance_by_odds(min_odds = None, max_odds = None, df = None):
    df = df[df['average_market_odds_old'].astype(float) >= min_odds]
    df = df[df['average_market_odds_old'].astype(float) <= max_odds]
    return df


@api.route('/api/data')
def get_test_data():
    df = pd.read_csv('market_view_data/market_view_data.csv')
    df.sort_values(by = 'prop_value', ascending=True, inplace=True)
    json_data = df.to_json(orient='records')
    return jsonify(json.loads(json_data))

@api.route('/api/chat/get_dfs_for_chat')
def get_dfs_for_chat():
    pos_ev_df = read_cached_df('pos_ev_dash_cache')
    arb_df = read_cached_df('arb_dash_cache')
    pregame = read_cached_df('ai_dash_cache')


@api.route('/api/performance_markets')
def performance_markets():
    try:
        engine = current_app.db_manager.get_engine()

        # Define the SQL query to select unique market_display values
        query = "SELECT DISTINCT market_display FROM ai_ev_graded"

        # Execute the query and load the data into a DataFrame
        df = pd.read_sql(query, engine)

        # Extract unique market_display values
        unique_market_display_values = df['market_display'].tolist()

        # Return the values as a JSON response
        return jsonify({'unique_market_display_values': unique_market_display_values})

    except Exception as e:
        # Handle any exceptions that occur during the database query or data processing
        return jsonify({'error': str(e)}), 500
    

@api.route('/api/player_prop_query', methods=['POST', 'GET'])
def player_prop_query():
    player_name = request.args.get('playerName')
    stat_column = request.args.get('statColumn')
    stat_threshold = float(request.args.get('statThreshold'))
    opponent = request.args.get('opponent')
    no_vig_odds = float(request.args.get('noVigOdds'))
    over_under = request.args.get('outcomeType')
    home_team = request.args.get('homeTeam')
    away_team = request.args.get('awayTeam')

    target_threshold = no_vig_odds

    try:
            
            logger.info(f"Player name recieved: {player_name}")
            logger.info(f"stat_column recieved: {stat_column}")
            logger.info(f"stat_threshold recieved: {stat_threshold}")
            logger.info(f"opponent recieved: {opponent}")
            logger.info(f"no_vig_odds recieved: {no_vig_odds}")
            logger.info(f"over_under recieved: {over_under}")
            logger.info(f"home_team recieved: {home_team}")
            logger.info(f"away_team recieved: {away_team}")

            response = make_query(
                player_name=player_name, 
                stat_column=stat_column, 
                stat_threshold=stat_threshold, 
                home_team=home_team,
                away_team=away_team,
                over_under=over_under,
                target_threshold=target_threshold,
                games_in_past=20,
            ) 

            logger.info(response)


            return jsonify({'message': response})
    except Exception as e:
        # Handle any exceptions that occur during the database query or data processing
        return jsonify({'error': str(e)}), 500



def read_cached_df(path):
    serialized_df = redis_client.get(path)
    if serialized_df:
      serialized_df = serialized_df.decode('utf-8')
      cached_df = pd.read_json(serialized_df)
      return cached_df
    else:
      raise FileNotFoundError(f"No cached DataFrame found at path: {path}")
