import pandas as pd
import redis
from redis.exceptions import ConnectionError

redis_client = redis.Redis(host='localhost', port=6379, db=0)

def update_dataframe_and_cache_in_batches(dataframe, path, batch_size=1000):
    for start in range(0, len(dataframe), batch_size):
        end = start + batch_size
        batch_df = dataframe.iloc[start:end]
        serialized_df = batch_df.to_json()
        try:
            redis_client.set(f"{path}_{start//batch_size}", serialized_df)
        except ConnectionError as e:
            print(f"Connection error: {e}")

if __name__ == "__main__":
    model_dash_cache = pd.read_csv('ai_model_output/mlb_07_18_2024_model.csv', low_memory=False)
    print("Reading and caching AI model dataframes...\n")
    model_dash_cache = model_dash_cache[model_dash_cache['completed'] == 0]
    update_dataframe_and_cache_in_batches(model_dash_cache, 'ai_dash_cache')
    print("\nAll cached AI dataframes reset!\n")