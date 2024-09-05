import pandas as pd
import flock as flock


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

if __name__ == "__main__":
    
    nba_df = pd.read_csv("market_view_data/nba_market_view_data.csv")
    nhl_df = pd.read_csv("market_view_data/nhl_market_view_data_BLANK.csv")

    shared_columns = nba_df.columns.intersection(nhl_df.columns)


    # nncaab_df = pd.read_csv("market_view_data/ncaab_market_view_data.csv")

    combined_df = pd.concat([nba_df[shared_columns], nhl_df[shared_columns]], ignore_index=True)

    while True:
      try:
        with open("market_view_data/nba_market_view_data.csv", 'r') as f:
            lock = flock.Flock(f, flock.LOCK_SH)
            with lock:
                new_nba_df = pd.read_csv(f)

        with open("market_view_data/nhl_market_view_data.csv", 'r') as f:
            lock = flock.Flock(f, flock.LOCK_SH)
            with lock:
                new_nhl_df = pd.read_csv(f)

        # with open("market_view_data/ncaab_market_view_data.csv", 'r') as f:
        #     lock = flock.Flock(f, flock.LOCK_SH)
        #     with lock:
        #         new_nncaab_df = pd.read_csv(f)

        shared_columns = nba_df.columns.intersection(nhl_df.columns)

        new_combined_df = pd.concat([new_nba_df[shared_columns], new_nhl_df[shared_columns]], ignore_index=True)

        if not are_dataframes_equal(combined_df, new_combined_df):
            combined_df = new_combined_df

            filename = "market_view_data/market_view_data.csv"

            with open(filename, 'w') as f:

                lock = flock.Flock(f, flock.LOCK_EX)
            
                with lock:
                    combined_df.to_csv(f, index=False)

            print("Updated")
        else:
            print("Not updated")

      except Exception as e:
          print(e)
          

      