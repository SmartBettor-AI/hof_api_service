import datetime
import pandas as pd
import time
import shutil
from util import read_cached_df

source_cache_name = 'pos_ev_dash_cache'

def copy_csv(source, destination):
    df = read_cached_df(source_cache_name)
    df.to_csv(destination, index=False)

while True:
    current_datetime = datetime.datetime.now()

    formatted_datetime = current_datetime.strftime("%m_%d_%Y_%H_%M_%S")
    
    destination_csv = f'pos_ev_data/pos_ev_data_{formatted_datetime}.csv'

    try:
      copy_csv(source_cache_name, destination_csv)
      print(f"CSV copied successfully to {destination_csv}.")
      time.sleep(5 * 60)
    except Exception as e:
       print(e)