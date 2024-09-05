import pandas as pd
import numpy as np
from model_db_manager import DBManager
import time
import warnings
warnings.filterwarnings("ignore")

class observation_compiler():
  def __init__(self):

    self.db_manager = DBManager()
    
    self.current_amount_of_nfl_observations = self.get_amount_of_master_sport_obs("NFL")

    self.current_amount_of_mlb_observations = self.get_amount_of_master_sport_obs("MLB")

    self.current_amount_of_nba_observations = self.get_amount_of_master_sport_obs("NBA")

    self.current_amount_of_nhl_observations = self.get_amount_of_master_sport_obs("NHL")

    self.current_amount_of_nhl_observations_pregame = self.get_amount_of_master_sport_obs("NHL_PREGAME")

    self.current_amount_of_nba_observations_pregame = self.get_amount_of_master_sport_obs("NBA_PREGAME")

    self.current_amount_of_nfl_observations_pregame = self.get_amount_of_master_sport_obs("NFL_PREGAME")

    self.current_amount_of_mlb_observations_pregame = self.get_amount_of_master_sport_obs("MLB_PREGAME")


    try:
      session = self.db_manager.create_session()

      query = "SELECT * FROM master_model_observations"

      engine = self.db_manager.get_engine()

      self.master_observations_sheet = pd.read_sql_query(query, engine)

    except Exception as e:
      print(e)
      raise
    finally:
      session.close()

    self.schema = ['sport_title', 'completed','game_id', 'game_date', 'team', 'minutes_since_commence', 'opponent', 'snapshot_time', 'ev', 'average_market_odds', 'highest_bettable_odds', 'sportsbooks_used', 'new_column']

  def compile_observations(self):

    nfl_obs = self.read_file('users/model_obs_nfl.csv')
    mlb_obs = self.read_file('users/model_obs_mlb.csv')
    nba_obs = self.read_file('users/model_obs_nba.csv')
    nhl_obs = self.read_file('users/model_obs_nhl.csv')
    nhl_obs_pregame = self.read_file('users/model_obs_nhl_pregame.csv')
    nba_obs_pregame = self.read_file('users/model_obs_nba_pregame.csv')
    nfl_obs_pregame = self.read_file('users/model_obs_nfl_pregame.csv')
    mlb_obs_pregame = self.read_file('users/model_obs_mlb_pregame.csv')

    self.current_amount_of_nfl_observations = self.get_amount_of_master_sport_obs("NFL")
    self.current_amount_of_mlb_observations = self.get_amount_of_master_sport_obs("MLB")
    self.current_amount_of_nba_observations = self.get_amount_of_master_sport_obs("NBA")
    self.current_amount_of_nhl_observations = self.get_amount_of_master_sport_obs("NHL")
    self.current_amount_of_nhl_observations_pregame = self.get_amount_of_master_sport_obs("NHL_PREGAME")
    self.current_amount_of_nba_observations_pregame = self.get_amount_of_master_sport_obs("NBA_PREGAME")
    self.current_amount_of_nfl_observations_pregame = self.get_amount_of_master_sport_obs("NFL_PREGAME")
    self.current_amount_of_mlb_observations_pregame = self.get_amount_of_master_sport_obs("MLB_PREGAME")


    if len(nfl_obs) > self.current_amount_of_nfl_observations:
      new_obs = pd.DataFrame()

      amount_of_new_observations = len(nfl_obs) - self.current_amount_of_nfl_observations

      new_obs = nfl_obs.tail(amount_of_new_observations).copy()

      new_obs['sport_title'] = 'NFL'

      new_obs['team'] = new_obs['team_1']

      new_obs['completed'] = False

      new_obs['game_date'] = pd.to_datetime(new_obs['commence_time']).dt.date

      new_obs['new_column'] = new_obs['game_id'].astype(str) + new_obs['snapshot_time'].astype(str)

      new_df = pd.DataFrame()

      new_df = new_obs[self.schema]

      try:
          new_df.to_sql('master_model_observations', con=self.db_manager.get_engine(), if_exists='append', index=False)
      except Exception as e:
        print(e)

    if len(nfl_obs_pregame) > self.current_amount_of_nfl_observations_pregame:
      new_obs = pd.DataFrame()

      amount_of_new_observations = len(nfl_obs_pregame) - self.current_amount_of_nfl_observations_pregame

      new_obs = nfl_obs_pregame.tail(amount_of_new_observations).copy()

      new_obs['sport_title'] = 'NFL_PREGAME'

      new_obs['team'] = new_obs['team_1']

      new_obs['completed'] = False

      new_obs['game_date'] = pd.to_datetime(new_obs['commence_time']).dt.date

      new_obs['new_column'] = new_obs['game_id'].astype(str) + new_obs['snapshot_time'].astype(str)

      new_df = pd.DataFrame()

      new_df = new_obs[self.schema]

      try:
          new_df.to_sql('master_model_observations', con=self.db_manager.get_engine(), if_exists='append', index=False)
      except Exception as e:
        print(e)
    
    if len(nba_obs) > self.current_amount_of_nba_observations:
      new_obs = pd.DataFrame()

      amount_of_new_observations = len(nba_obs) - self.current_amount_of_nba_observations

      new_obs = nba_obs.tail(amount_of_new_observations).copy()

      new_obs['sport_title'] = 'NBA'

      new_obs['team'] = new_obs['team_1']

      new_obs['completed'] = False

      new_obs['game_date'] = pd.to_datetime(new_obs['commence_time']).dt.date

      new_obs['new_column'] = new_obs['game_id'].astype(str) + new_obs['snapshot_time'].astype(str)

      new_df = pd.DataFrame()

      new_df = new_obs[self.schema]

      try:
          new_df.to_sql('master_model_observations', con=self.db_manager.get_engine(), if_exists='append', index=False)
      except Exception as e:
        print(e)
    
    if len(nba_obs_pregame) > self.current_amount_of_nba_observations_pregame:
      new_obs = pd.DataFrame()

      amount_of_new_observations = len(nba_obs_pregame) - self.current_amount_of_nba_observations_pregame

      new_obs = nba_obs_pregame.tail(amount_of_new_observations).copy()

      new_obs['sport_title'] = 'NBA_PREGAME'

      new_obs['team'] = new_obs['team_1']

      new_obs['completed'] = False

      new_obs['game_date'] = pd.to_datetime(new_obs['commence_time']).dt.date

      new_obs['new_column'] = new_obs['game_id'].astype(str) + new_obs['snapshot_time'].astype(str)

      new_df = pd.DataFrame()

      new_df = new_obs[self.schema]

      try:
          new_df.to_sql('master_model_observations', con=self.db_manager.get_engine(), if_exists='append', index=False)
      except Exception as e:
        print(e)
    
    if len(nhl_obs) > self.current_amount_of_nhl_observations:
      new_obs = pd.DataFrame()

      amount_of_new_observations = len(nhl_obs) - self.current_amount_of_nhl_observations

      new_obs = nhl_obs.tail(amount_of_new_observations).copy()

      new_obs['sport_title'] = 'NHL'

      new_obs['team'] = new_obs['team_1']

      new_obs['completed'] = False

      new_obs['game_date'] = pd.to_datetime(new_obs['commence_time']).dt.date

      new_obs['new_column'] = new_obs['game_id'].astype(str) + new_obs['snapshot_time'].astype(str)

      new_df = pd.DataFrame()

      new_df = new_obs[self.schema]

      try:
          new_df.to_sql('master_model_observations', con=self.db_manager.get_engine(), if_exists='append', index=False)
      except Exception as e:
        print(e)
    
    if len(nhl_obs_pregame) > self.current_amount_of_nhl_observations_pregame:
      new_obs = pd.DataFrame()

      amount_of_new_observations = len(nhl_obs_pregame) - self.current_amount_of_nhl_observations_pregame

      new_obs = nhl_obs_pregame.tail(amount_of_new_observations).copy()

      new_obs['sport_title'] = 'NHL_PREGAME'

      new_obs['team'] = new_obs['team_1']

      new_obs['completed'] = False

      new_obs['game_date'] = pd.to_datetime(new_obs['commence_time']).dt.date

      new_obs['new_column'] = new_obs['game_id'].astype(str) + new_obs['snapshot_time'].astype(str)

      new_df = pd.DataFrame()

      new_df = new_obs[self.schema]

      try:
          new_df.to_sql('master_model_observations', con=self.db_manager.get_engine(), if_exists='append', index=False)
      except Exception as e:
        print(e)

    if len(mlb_obs) > self.current_amount_of_mlb_observations:
      new_obs = pd.DataFrame()

      amount_of_new_observations = len(mlb_obs) - self.current_amount_of_mlb_observations

      new_obs = mlb_obs.tail(amount_of_new_observations).copy()

      new_obs['sport_title'] = 'MLB'

      new_obs['team'] = new_obs['team_1']

      new_obs['completed'] = False

      new_obs['game_date'] = pd.to_datetime(new_obs['commence_time']).dt.date

      new_obs['new_column'] = new_obs['game_id'].astype(str) + new_obs['snapshot_time'].astype(str)

      new_df = pd.DataFrame()

      new_df = new_obs[self.schema]

      try:
          new_df.to_sql('master_model_observations', con=self.db_manager.get_engine(), if_exists='append', index=False)
      except Exception as e:
        print(e)

    if len(mlb_obs_pregame) > self.current_amount_of_mlb_observations_pregame:
      new_obs = pd.DataFrame()

      amount_of_new_observations = len(mlb_obs_pregame) - self.current_amount_of_mlb_observations_pregame

      new_obs = mlb_obs_pregame.tail(amount_of_new_observations).copy()

      new_obs['sport_title'] = 'MLB_PREGAME'

      new_obs['team'] = new_obs['team_1']

      new_obs['completed'] = False

      new_obs['game_date'] = pd.to_datetime(new_obs['commence_time']).dt.date

      new_obs['new_column'] = new_obs['game_id'].astype(str) + new_obs['snapshot_time'].astype(str)

      new_obs['ev'] = 0

      new_df = pd.DataFrame()

      new_df = new_obs[self.schema]

      try:
          new_df.to_sql('master_model_observations', con=self.db_manager.get_engine(), if_exists='append', index=False)
      except Exception as e:
        print(e)

  def update_completed_observations(self):
    try:
          session = self.db_manager.create_session()
          scores =  pd.read_sql_table('scores', con=self.db_manager.get_engine())
    except Exception as e:
        print(e)
    finally:
        session.close()

    completed_ids = scores['game_id'].unique().tolist()

    non_updated_master_model_obs_sheet = self.master_observations_sheet.copy()

    self.master_observations_sheet['completed'] = np.where(
        self.master_observations_sheet['game_id'].isin(completed_ids),
        True,
        False
      )

    if non_updated_master_model_obs_sheet['completed'].sum() == self.master_observations_sheet['completed'].sum():
      return
    else:
      try:
            self.master_observations_sheet.to_sql('master_model_observations', con=self.db_manager.get_engine(), if_exists='replace', index=False)
      except Exception as e:
          print(e)
          return 
      finally:
        if 'session' in locals():
            session.close()

  def get_amount_of_master_sport_obs(self, sport_title):
    try:
          start_time = time.time()
          session = self.db_manager.create_session()
          query = """
            SELECT COUNT(*) 
            FROM master_model_observations
            WHERE sport_title = :sport_title
            """
          engine = self.db_manager.get_engine()
          result = session.execute(query, {"sport_title": sport_title})
          length = result.scalar()
          session.close()
          end_time = time.time()
          print(f"master length: {length}")
        
    except Exception as e:
      print(e)
    finally:
      # session.close()
      return length
  
  def read_file(self, filename):
     
     while True:

      try:
          df = pd.read_csv(filename)
          return df
      
      except pd.errors.EmptyDataError:
        print("empty data error")
        pass
      
      except pd.errors.ParserError as e:
        print(f"Parse error in {filename}: ", e)

        pass
          
      except Exception as e:
        print(e)
        pass

if __name__ == "__main__":
   
   while True:
      try:

        observation_compiler_instance = observation_compiler()

        print("about to start compiling...")

        observation_compiler_instance.compile_observations()
   
        observation_compiler_instance.update_completed_observations()
      
      except Exception as e:
         
         print(e)
   
