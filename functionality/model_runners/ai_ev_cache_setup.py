import pandas as pd
import redis
redis_client = redis.Redis(host='localhost', port=6379, db=0)

def update_dataframe_and_cache(dataframe, path):
    serialized_df = dataframe.to_json()
    redis_client.set(path, serialized_df)


if __name__ == "__main__":

  ai_ev_dash_cache = pd.DataFrame()
  mlb_ai_ev_pregame_cache = pd.DataFrame()
  ncaaf_ai_ev_pregame_cache = pd.DataFrame()
  nfl_ai_ev_pregame_cache = pd.DataFrame()

  update_dataframe_and_cache(ai_ev_dash_cache, 'ai_ev_dash_cache')
  update_dataframe_and_cache(mlb_ai_ev_pregame_cache, 'mlb_ai_ev_pregame_cache')
  update_dataframe_and_cache(ncaaf_ai_ev_pregame_cache, 'ncaaf_ai_ev_pregame_cache')
  update_dataframe_and_cache(nfl_ai_ev_pregame_cache, 'nfl_ai_ev_pregame_cache')

  print()
  print("All cached dataframes reset!")
  print()