import pandas as pd
import flock as flock
import redis
import time
from db_manager import DBManager
from sqlalchemy import create_engine, text
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import sqlalchemy
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import inspect


redis_client = redis.Redis(host='localhost', port=6379, db=0)

def are_dataframes_equal(df1, df2):
    """Check if two DataFrames are equal."""
    return df1.equals(df2)


def replace_dataframe_if_different(original_df, new_df):
    """Replace original DataFrame if it's different from the new one."""
    if not are_dataframes_equal(original_df, new_df):
        print("DataFrames are different. Replacing the original DataFrame.")
        return new_df
    else:
        print("DataFrames are the same. Keeping the original DataFrame.")
        return original_df


def update_dataframe_and_cache(dataframe):
    dataframe.to_csv('pos_ev_cache_current.csv', index = False)
    serialized_df = dataframe.to_json()
    redis_client.set('pos_ev_dash_cache', serialized_df)


def read_cached_df(path):
    serialized_df = redis_client.get(path)
    if serialized_df:
      serialized_df = serialized_df.decode('utf-8')
      cached_df = pd.read_json(serialized_df)
      return cached_df
    else:
      raise FileNotFoundError(f"No cached DataFrame found at path: {path}")
    

import pandas as pd
from sqlalchemy import inspect, types
from sqlalchemy.exc import SQLAlchemyError

def write_to_sql(df, table_name, engine, if_exists='append'):
    try:
        # Get the table schema
        inspector = inspect(engine)
        columns = inspector.get_columns(table_name)
        
        # Create a dictionary of column names and their SQLAlchemy types
        dtype_dict = {col['name']: col['type'] for col in columns}
        
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



if __name__ == "__main__":
    
    nba_df = read_cached_df('nba_pos_ev_cache')
    nhl_df = read_cached_df('nhl_pos_ev_cache')
    mlb_df = read_cached_df('mlb_pos_ev_cache')
    wnba_df = read_cached_df('wnba_pos_ev_cache')
    pll_df = read_cached_df('pll_pos_ev_cache')
    soccer_africa_cup_of_nations_df = read_cached_df('soccer_africa_cup_of_nations_pos_ev_cache')
    soccer_conmebol_copa_america_df = read_cached_df('soccer_conmebol_copa_america_pos_ev_cache')
    soccer_epl_df = read_cached_df('soccer_epl_pos_ev_cache')
    soccer_germany_bundesliga_df = read_cached_df('soccer_germany_bundesliga_pos_ev_cache')
    soccer_italy_serie_a_df = read_cached_df('soccer_italy_serie_a_pos_ev_cache')
    soccer_spain_la_liga_df = read_cached_df('soccer_spain_la_liga_pos_ev_cache')
    soccer_uefa_champs_league_df = read_cached_df('soccer_uefa_champs_league_pos_ev_cache')
    soccer_uefa_champs_league_qualification_df = read_cached_df('soccer_uefa_champs_league_qualification_pos_ev_cache')
    soccer_uefa_europa_league_df = read_cached_df('soccer_uefa_europa_league_pos_ev_cache')
    soccer_usa_mls_df = read_cached_df('soccer_usa_mls_pos_ev_cache')
    soccer_uefa_european_championship_df = read_cached_df('soccer_uefa_european_championship_pos_ev_cache')
    tennis_atp_aus_open_singles_df = read_cached_df('tennis_atp_aus_open_singles_pos_ev_cache')
    tennis_atp_us_open_df = read_cached_df('tennis_atp_us_open_pos_ev_cache')
    tennis_atp_wimbledon_df = read_cached_df('tennis_atp_wimbledon_pos_ev_cache')
    tennis_wta_us_open_df = read_cached_df('tennis_wta_us_open_pos_ev_cache')
    tennis_atp_french_open_df = read_cached_df('tennis_atp_french_open_pos_ev_cache')
    tennis_wta_wimbledon_df = read_cached_df('tennis_wta_wimbledon_pos_ev_cache')
    mma_df = read_cached_df('mma_mixed_martial_arts_pos_ev_cache')
    nfl_df = read_cached_df('americanfootball_nfl_pos_ev_cache')
    ncaaf_df = read_cached_df('americanfootball_ncaaf_pos_ev_cache')

    db_manager = DBManager()

    combined_df = pd.concat([
    nba_df, 
    nhl_df, 
    mlb_df, 
    wnba_df, 
    pll_df, 
    soccer_africa_cup_of_nations_df, 
    soccer_conmebol_copa_america_df, 
    soccer_epl_df, 
    soccer_germany_bundesliga_df, 
    soccer_italy_serie_a_df, 
    soccer_spain_la_liga_df, 
    soccer_uefa_champs_league_df, 
    soccer_uefa_champs_league_qualification_df, 
    soccer_uefa_europa_league_df, 
    soccer_usa_mls_df, 
    soccer_uefa_european_championship_df, 
    tennis_atp_aus_open_singles_df, 
    tennis_atp_us_open_df, 
    tennis_atp_wimbledon_df, 
    tennis_wta_us_open_df, 
    tennis_atp_french_open_df, 
    tennis_wta_wimbledon_df,
    mma_df,
    nfl_df,
    ncaaf_df,
            ], ignore_index=True)


    while True:
      try:

        new_mlb_df = read_cached_df('mlb_pos_ev_cache')

        new_nba_df = read_cached_df('nba_pos_ev_cache')

        new_nhl_df = read_cached_df('nhl_pos_ev_cache')

        new_wnba_df = read_cached_df('wnba_pos_ev_cache')

        new_pll_df = read_cached_df('pll_pos_ev_cache')

        new_soccer_africa_cup_of_nations_df = read_cached_df('soccer_africa_cup_of_nations_pos_ev_cache')

        new_soccer_conmebol_copa_america_df = read_cached_df('soccer_conmebol_copa_america_pos_ev_cache')

        new_soccer_epl_df = read_cached_df('soccer_epl_pos_ev_cache')

        new_soccer_germany_bundesliga_df = read_cached_df('soccer_germany_bundesliga_pos_ev_cache')

        new_soccer_italy_serie_a_df = read_cached_df('soccer_italy_serie_a_pos_ev_cache')

        new_soccer_spain_la_liga_df = read_cached_df('soccer_spain_la_liga_pos_ev_cache')

        new_soccer_uefa_champs_league_df = read_cached_df('soccer_uefa_champs_league_pos_ev_cache')

        new_soccer_uefa_champs_league_qualification_df = read_cached_df('soccer_uefa_champs_league_qualification_pos_ev_cache')

        new_soccer_uefa_europa_league_df = read_cached_df('soccer_uefa_europa_league_pos_ev_cache')

        new_soccer_usa_mls_df = read_cached_df('soccer_usa_mls_pos_ev_cache')

        new_soccer_uefa_european_championship_df = read_cached_df('soccer_uefa_european_championship_pos_ev_cache')

        new_tennis_atp_aus_open_singles_df = read_cached_df('tennis_atp_aus_open_singles_pos_ev_cache')

        new_tennis_atp_us_open_df = read_cached_df('tennis_atp_us_open_pos_ev_cache')

        new_tennis_atp_wimbledon_df = read_cached_df('tennis_atp_wimbledon_pos_ev_cache')

        new_tennis_wta_us_open_df = read_cached_df('tennis_wta_us_open_pos_ev_cache')

        new_tennis_atp_french_open_df = read_cached_df('tennis_atp_french_open_pos_ev_cache')

        new_tennis_wta_wimbledon_df = read_cached_df('tennis_wta_wimbledon_pos_ev_cache')

        new_mma_df = read_cached_df('mma_mixed_martial_arts_pos_ev_cache')
        
        new_nfl_df = read_cached_df('americanfootball_nfl_pos_ev_cache')

        new_ncaaf_df = read_cached_df('americanfootball_ncaaf_pos_ev_cache')

        new_combined_df = pd.concat([
            new_nba_df, 
            new_nhl_df, 
            new_mlb_df, 
            new_wnba_df, 
            new_pll_df, 
            new_soccer_africa_cup_of_nations_df, 
            new_soccer_conmebol_copa_america_df, 
            new_soccer_epl_df, 
            new_soccer_germany_bundesliga_df, 
            new_soccer_italy_serie_a_df, 
            new_soccer_spain_la_liga_df, 
            new_soccer_uefa_champs_league_df, 
            new_soccer_uefa_champs_league_qualification_df, 
            new_soccer_uefa_europa_league_df, 
            new_soccer_usa_mls_df, 
            new_soccer_uefa_european_championship_df, 
            new_tennis_atp_aus_open_singles_df, 
            new_tennis_atp_us_open_df, 
            new_tennis_atp_wimbledon_df, 
            new_tennis_wta_us_open_df, 
            new_tennis_atp_french_open_df, 
            new_tennis_wta_wimbledon_df,
            new_mma_df,
            new_nfl_df,
            new_ncaaf_df,
        ], ignore_index=True)


        if not are_dataframes_equal(combined_df, new_combined_df):
            combined_df = new_combined_df
            combined_df.to_csv('combined_df.csv', index=False)

            update_dataframe_and_cache(combined_df)

            # write_to_sql(table_name='ev_observations', df = combined_df, if_exists = 'append', engine=  db_manager.get_engine())

            print("Updated")
        else:
            print("Not updated")

      except Exception as e:
          print(e)
          

      