import pandas as pd
import flock as flock
import redis
import time
import warnings
warnings.filterwarnings("ignore")

redis_client = redis.Redis(host='localhost', port=6379, db=0)

class AIEVModelCacher():
    
    def __init__(self):
        self = self
        

    def are_dataframes_equal(self, df1, df2):
      """Check if two DataFrames are equal."""
      return df1.equals(df2)


    def update_dataframe_and_cache(self, dataframe):
      serialized_df = dataframe.to_json()
      redis_client.set('ai_ev_dash_cache', serialized_df)
      print("\nAI EV Dash Cache Set!\n")


    def read_cached_df(self, path):
        serialized_df = redis_client.get(path)
        if serialized_df:
          serialized_df = serialized_df.decode('utf-8')
          cached_df = pd.read_json(serialized_df)
          return cached_df
        
        else:
          return pd.DataFrame()


    def run(self):
        
        mlb_df = pd.DataFrame()

        ncaaf_df =  pd.DataFrame()

        nfl_df = pd.DataFrame()

        combined_df = pd.concat([mlb_df, ncaaf_df, nfl_df], ignore_index=True)

        while True: 
            
            try:
            
              new_mlb_df = self.read_cached_df('mlb_ai_ev_pregame_cache')

              new_ncaaf_df =  self.read_cached_df('ncaaf_ai_ev_pregame_cache')

              new_nfl_df =  self.read_cached_df('nfl_ai_ev_pregame_cache')
              
              new_combined_df = pd.concat([new_mlb_df, new_ncaaf_df, new_nfl_df], ignore_index=True)

              if not self.are_dataframes_equal(new_combined_df, combined_df):
                  
                  self.update_dataframe_and_cache(new_combined_df)

                  combined_df = new_combined_df
              else:
                print("\nNo updates...\n")

            except Exception as e:
               print(e)
               time.sleep(1)

if __name__ == "__main__":
   cacher = AIEVModelCacher()
   cacher.run()