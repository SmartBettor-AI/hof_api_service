import pandas as pd
import redis
redis_client = redis.Redis(host='localhost', port=6379, db=0)

def update_dataframe_and_cache(dataframe, path):
    serialized_df = dataframe.to_json()
    redis_client.set(path, serialized_df)


if __name__ == "__main__":

  ai_dash_cache = pd.read_csv('ai_model_output/mlb_full_markets_model.csv')

  update_dataframe_and_cache(ai_dash_cache, 'ai_dash_cache')

  print()
  print("All cached dataframes reset!")
  print()