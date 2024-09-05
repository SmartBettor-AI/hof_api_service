import pandas as pd
import flock as flock
import redis
import time

redis_client = redis.Redis(host='localhost', port=6379, db=0)

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

def update_dataframe_and_cache(dataframe):
    serialized_df = dataframe.to_json()
    redis_client.set('arb_dash_cache', serialized_df)


def read_cached_df(path):
    serialized_df = redis_client.get(path)
    if serialized_df:
      serialized_df = serialized_df.decode('utf-8')
      cached_df = pd.read_json(serialized_df)
      return cached_df
    else:
      raise FileNotFoundError(f"No cached DataFrame found at path: {path}")

if __name__ == "__main__":
    
    nba_df = read_cached_df('nba_arb_cache')
    nhl_df = read_cached_df('nhl_arb_cache')
    mlb_df = read_cached_df('mlb_arb_cache')
    wnba_df = read_cached_df('wnba_arb_cache')
    pll_df = read_cached_df('pll_arb_cache')
    acon_df = read_cached_df('soccer_africa_cup_of_nations_arb_cache')
    copa_df = read_cached_df('soccer_conmebol_copa_america_arb_cache')
    epl_df = read_cached_df('soccer_epl_arb_cache')
    bunde_df = read_cached_df('soccer_germany_bundesliga_arb_cache')
    seriea_df = read_cached_df('soccer_italy_serie_a_arb_cache')
    laliga_df = read_cached_df('soccer_spain_la_liga_arb_cache')
    uefachamps_df = read_cached_df('soccer_uefa_champs_league_arb_cache')
    uefachampsq_df = read_cached_df('soccer_uefa_champs_league_qualification_arb_cache')
    uefaeuropa_df = read_cached_df('soccer_uefa_europa_league_arb_cache')
    mls_df = read_cached_df('soccer_usa_mls_arb_cache')
    euros_df = read_cached_df('soccer_uefa_european_championship_arb_cache')
    ausopenmens1_df = read_cached_df('tennis_atp_aus_open_singles_arb_cache')
    usopenmens1_df = read_cached_df('tennis_atp_us_open_arb_cache')
    wimbledonmens1_df = read_cached_df('tennis_atp_wimbledon_arb_cache')
    usopenwomens1_df = read_cached_df('tennis_wta_us_open_arb_cache')
    frenchopenmens1_df = read_cached_df('tennis_atp_french_open_arb_cache')
    wimbledonwomens1_df = read_cached_df('tennis_wta_wimbledon_arb_cache')
    mma_df = read_cached_df('mma_mixed_martial_arts_arb_cache')
    nfl_df = read_cached_df('americanfootball_nfl_arb_cache')
    ncaaf_df = read_cached_df('americanfootball_ncaaf_arb_cache')
    combined_df = pd.concat([nba_df, nhl_df, mlb_df, wnba_df, pll_df, acon_df, copa_df, epl_df, bunde_df, seriea_df, laliga_df, uefachamps_df,
                            uefachampsq_df, uefaeuropa_df, mls_df,  euros_df, ausopenmens1_df, usopenmens1_df, wimbledonmens1_df, usopenwomens1_df, 
                            frenchopenmens1_df, wimbledonwomens1_df, mma_df, nfl_df, ncaaf_df], ignore_index=True)

    while True:
      try:

        new_mlb_df = read_cached_df('mlb_arb_cache')

        new_nba_df = read_cached_df('nba_arb_cache')

        new_nhl_df = read_cached_df('nhl_arb_cache')

        new_wnba_df = read_cached_df('wnba_arb_cache')

        new_pll_df = read_cached_df('pll_arb_cache')

        new_soccer_africa_cup_of_nations_df = read_cached_df('soccer_africa_cup_of_nations_arb_cache')

        new_soccer_conmebol_copa_america_df = read_cached_df('soccer_conmebol_copa_america_arb_cache')

        new_soccer_epl_df = read_cached_df('soccer_epl_arb_cache')

        new_soccer_germany_bundesliga_df = read_cached_df('soccer_germany_bundesliga_arb_cache')

        new_soccer_italy_serie_a_df = read_cached_df('soccer_italy_serie_a_arb_cache')

        new_soccer_spain_la_liga_df = read_cached_df('soccer_spain_la_liga_arb_cache')

        new_soccer_uefa_champs_league_df = read_cached_df('soccer_uefa_champs_league_arb_cache')

        new_soccer_uefa_champs_league_qualification_df = read_cached_df('soccer_uefa_champs_league_qualification_arb_cache')

        new_soccer_uefa_europa_league_df = read_cached_df('soccer_uefa_europa_league_arb_cache')

        new_soccer_usa_mls_df = read_cached_df('soccer_usa_mls_arb_cache')

        new_soccer_uefa_european_championship_df = read_cached_df('soccer_uefa_european_championship_arb_cache')

        new_tennis_atp_aus_open_singles_df = read_cached_df('tennis_atp_aus_open_singles_arb_cache')

        new_tennis_atp_us_open_df = read_cached_df('tennis_atp_us_open_arb_cache')

        new_tennis_atp_wimbledon_df = read_cached_df('tennis_atp_wimbledon_arb_cache')

        new_tennis_wta_us_open_df = read_cached_df('tennis_wta_us_open_arb_cache')

        new_tennis_atp_french_open_df = read_cached_df('tennis_atp_french_open_arb_cache')

        new_tennis_wta_wimbledon_df = read_cached_df('tennis_wta_wimbledon_arb_cache')
        
        new_mma_df = read_cached_df('mma_mixed_martial_arts_arb_cache')

        new_nfl_df = read_cached_df('americanfootball_nfl_arb_cache')

        new_ncaaf_df = read_cached_df('americanfootball_ncaaf_arb_cache')
                     

        new_combined_df = pd.concat([
        new_mlb_df,
        new_nba_df,
        new_nhl_df,
        new_wnba_df,
        new_pll_df,
        new_soccer_africa_cup_of_nations_df,
        new_soccer_conmebol_copa_america_df,
        new_soccer_epl_df,
        new_soccer_germany_bundesliga_df,
        new_soccer_italy_serie_a_df,
        new_soccer_spain_la_liga_df,
        new_soccer_uefa_champs_league_df,
        new_soccer_uefa_champs_league_qualification_df,
        new_soccer_uefa_europa_league_df,
        new_soccer_usa_mls_df,
        new_soccer_uefa_european_championship_df,
        new_tennis_atp_aus_open_singles_df,
        new_tennis_atp_us_open_df,
        new_tennis_atp_wimbledon_df,
        new_tennis_wta_us_open_df,
        new_tennis_atp_french_open_df,
        new_tennis_wta_wimbledon_df,
        new_mma_df,
        new_nfl_df,
        new_ncaaf_df
        ], ignore_index=True)
        new_combined_df.to_csv('new_combined_df.csv', index = False)

        if not are_dataframes_equal(combined_df, new_combined_df):
            combined_df = new_combined_df

            update_dataframe_and_cache(combined_df)

            print("Updated")
        else:
            print("Not updated")

      except Exception as e:
          print(e)
          

      