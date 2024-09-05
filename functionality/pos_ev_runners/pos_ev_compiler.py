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

# Example usage:
if __name__ == "__main__":
    
    nba_df = pd.read_csv("pos_ev_data/nba_pos_ev_data.csv")
    nhl_df = pd.read_csv("pos_ev_data/nhl_pos_ev_data.csv")
    nncaab_df = pd.read_csv("pos_ev_data/ncaab_pos_ev_data.csv")

    mlb_df = pd.read_csv("pos_ev_data/mlb_pos_ev_data.csv")


    combined_df = pd.concat([nba_df, nncaab_df, nhl_df, mlb_df], ignore_index=True)

    while True:
      try:

        with open("pos_ev_data/mlb_pos_ev_data.csv", 'r') as f:
            lock = flock.Flock(f, flock.LOCK_SH)
            with lock:
                new_mlb_df = pd.read_csv(f)

        with open("pos_ev_data/nba_pos_ev_data.csv", 'r') as f:
            lock = flock.Flock(f, flock.LOCK_SH)
            with lock:
                new_nba_df = pd.read_csv(f)

        with open("pos_ev_data/nhl_pos_ev_data.csv", 'r') as f:
            lock = flock.Flock(f, flock.LOCK_SH)
            with lock:
                new_nhl_df = pd.read_csv(f)

        with open("pos_ev_data/ncaab_pos_ev_data.csv", 'r') as f:
            lock = flock.Flock(f, flock.LOCK_SH)
            with lock:
                new_nncaab_df = pd.read_csv(f)

        new_combined_df = pd.concat([new_nba_df, new_nncaab_df, new_nhl_df, new_mlb_df], ignore_index=True)

        if not are_dataframes_equal(combined_df, new_combined_df):
            combined_df = new_combined_df

            with open("pos_ev_data/pos_ev_dash_data.csv", 'w') as f:
                lock = flock.Flock(f, flock.LOCK_EX)
                with lock:
                    combined_df.to_csv(f, index=False)

            print("Updated")
        else:
            print("Not updated")

      except Exception as e:
          print(e)
          

      