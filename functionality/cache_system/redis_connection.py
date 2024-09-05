import redis
import pandas as pd

redis_client = redis.Redis(host='localhost', port=6379, db=0)

def update_dataframe_and_cache(dataframe):
    serialized_df = dataframe.to_json()
    redis_client.set('cached_dataframe', serialized_df)


update_dataframe_and_cache(df)

# Retrieve DataFrame from Redis
serialized_df = redis_client.get('cached_dataframe')
cached_df = pd.read_json(serialized_df)

print("Cached DataFrame:")
print(cached_df)
