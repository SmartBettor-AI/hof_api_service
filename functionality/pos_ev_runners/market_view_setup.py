import pandas as pd
import redis
redis_client = redis.Redis(host='localhost', port=6379, db=0)

def update_dataframe_and_cache(dataframe, path):
    serialized_df = dataframe.to_json()
    redis_client.set(path, serialized_df)


if __name__ == "__main__":
  mma_market_view = pd.read_csv('market_view_data/mma_mixed_martial_arts_market_view_data.csv')
  update_dataframe_and_cache(mma_market_view, 'mma_mixed_martial_arts_market_view_cache')

  print()
  print("All +EV DataFrames re-cached!")
  print()