import pandas as pd
import flock as flock
import redis
import time

redis_client = redis.Redis(host='localhost', port=6379, db=0)

from model_db_manager import DBManager

db_manager = DBManager()

class AIModelCacher():
    def __init__(self):
        self = self
        
    def are_dataframes_equal(self, df1, df2):
      """Check if two DataFrames are equal."""
      return df1.equals(df2)


    def replace_dataframe_if_different(self, original_df, new_df):
      """Replace original DataFrame if it's different from the new one."""
      if not self.are_dataframes_equal(original_df, new_df):
          print("DataFrames are different. Replacing the original DataFrame.")
          return new_df
      else:
          print("DataFrames are the same. Keeping the original DataFrame.")
          return original_df


    def update_dataframe_and_cache(self, dataframe):
      serialized_df = dataframe.to_json()
      redis_client.set('ai_dash_cache', serialized_df)


    def get_completed_game_ids(self, df):
      
      try:
        session = db_manager.create_session()
        scores =  pd.read_sql_table('scores', con = db_manager.get_engine())

      except Exception as e:
            print(e)

      finally:
            session.close()
            completed_ids = scores['game_id'].unique().tolist()
            
      if 'session' in locals():
                session.close()

      return completed_ids


    def add_completed_column_and_save(self, df, name, completed_game_ids):
        
        df['completed'] = df['game_id'].isin(completed_game_ids).astype(int)

        return df


    def filter_for_display(self, df):
        new_df = df.groupby('outcome').last().reset_index()
        return new_df[new_df['completed'] == 0]


    def read_cached_df(self, path):
        serialized_df = redis_client.get(path)
        if serialized_df:
          serialized_df = serialized_df.decode('utf-8')
          cached_df = pd.read_json(serialized_df)
          return cached_df
        
        else:
          return pd.DataFrame()


    def perform_cache(self):

      # df0 = pd.read_csv('ai_model_output/mlb_player_only_model.csv')

      # df1 = pd.read_csv('ai_model_output/mlb_06_10_2024_model_v2.csv')

      # df2 = pd.read_csv('ai_model_output/mlb_06_11_2024_model_for_winnable.csv')

      # df3 = pd.read_csv('ai_model_output/mlb_06_11_2024_model_for_winnable_2.csv')

      df4 = pd.read_csv('ai_model_output/mlb_07_18_2024_model.csv')

      print('df4 is here')
      
      combined_df = pd.concat([df4], ignore_index=True)
      completed_ids = self.get_completed_game_ids(combined_df)
      combined_df = self.add_completed_column_and_save(
          df = df4, 
          name = 'ai_model_output/mlb_07_18_2024_model.csv', 
          completed_game_ids = completed_ids
      )
      combined_df.to_csv('ai_model_output/mlb_07_18_2024_model.csv', index = False)



      new_display_df = self.filter_for_display(combined_df)

      current_display_df = self.read_cached_df('ai_dash_cache')

      if not self.are_dataframes_equal(new_display_df, current_display_df):
          
        self.update_dataframe_and_cache(new_display_df)

        print(new_display_df)

        print("Updated")

      else:
          print("Not updated")

      # self.add_completed_column_and_save(
      #     df = df0, 
      #     name = 'ai_model_output/mlb_player_only_model.csv', 
      #     completed_game_ids = completed_ids
      # )

      # self.add_completed_column_and_save(
      #     df = df1, 
      #     name = 'ai_model_output/mlb_06_10_2024_model_v2.csv', 
      #     completed_game_ids = completed_ids
      # )

      # self.add_completed_column_and_save(
      #     df = df2, 
      #     name = 'ai_model_output/mlb_06_11_2024_model_for_winnable.csv', 
      #     completed_game_ids = completed_ids
      # )

      # self.add_completed_column_and_save(
      #     df = df3, 
      #     name = 'ai_model_output/mlb_06_11_2024_model_for_winnable_2.csv', 
      #     completed_game_ids = completed_ids
      # )
