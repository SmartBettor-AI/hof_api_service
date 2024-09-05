import redis
import pandas as pd
import json
import os
import time
redis_client = redis.Redis(host='localhost', port=6379, db=0)
import warnings
warnings.filterwarnings("ignore")



def read_cached_df(path):
    serialized_df = redis_client.get(path)
    if serialized_df:
      serialized_df = serialized_df.decode('utf-8')
      cached_df = pd.read_json(serialized_df)
      return cached_df
    else:
      raise FileNotFoundError(f"No cached DataFrame found at path: {path}")
    

def are_dataframes_equal(df1, df2):
    """Check if two DataFrames are equal."""
    return df1.equals(df2)


def update_dataframe_and_cache(dataframe, cache_path):
    serialized_df = dataframe.to_json()
    redis_client.set(cache_path, serialized_df)


class SportCacher():
   def __init__ (self, sport):
      self.sport = sport
      self.game_ids = self.get_game_ids_for_sport()


      self.ev_cache_paths = {
       "NBA": "nba_pos_ev_cache",
       "MLB": "mlb_pos_ev_cache",
       "NHL": "nhl_pos_ev_cache",
       "NCAAB": "ncaab_pos_ev_cache",
       "WNBA": "wnba_pos_ev_cache",
       "PLL": "pll_pos_ev_cache",
       "ACON": "soccer_africa_cup_of_nations_pos_ev_cache",
       "COPA": "soccer_conmebol_copa_america_pos_ev_cache",
       "EPL": "soccer_epl_pos_ev_cache",  
       "BUNDE": "soccer_germany_bundesliga_pos_ev_cache",
       "SERIEA": "soccer_italy_serie_a_pos_ev_cache",
       "LALIGA": "soccer_spain_la_liga_pos_ev_cache",
       "UEFACHAMPS": "soccer_uefa_champs_league_pos_ev_cache",
       "UEFACHAMPSQ": "soccer_uefa_champs_league_qualification_pos_ev_cache",
       "UEFAEUROPA": "soccer_uefa_europa_league_pos_ev_cache",
       "MLS": "soccer_usa_mls_pos_ev_cache",
       "EUROS": "soccer_uefa_european_championship_pos_ev_cache",
       "AUSOPENMENS1": "tennis_atp_aus_open_singles_pos_ev_cache",
       "USOPENMENS1": "tennis_atp_us_open_pos_ev_cache",
       "WIMBLEDONMENS1": "tennis_atp_wimbledon_pos_ev_cache",
       "USOPENWOMENS1": "tennis_wta_us_open_pos_ev_cache",
       "FRENCHOPENMENS1": "tennis_atp_french_open_pos_ev_cache",
       "WIMBLEDONWOMENS1": "tennis_wta_wimbledon_pos_ev_cache",
       "MMA": "mma_mixed_martial_arts_pos_ev_cache",
       "NFL": "americanfootball_nfl_pos_ev_cache",
       "CFB": "americanfootball_ncaaf_pos_ev_cache"
   
    }


      self.arb_cache_paths = {
       "NBA": "nba_arb_cache",
       "MLB": "mlb_arb_cache",
       "NHL": "nhl_arb_cache",
       "NCAAB": "ncaab_arb_cache",
       "WNBA": "wnba_arb_cache",
       "PLL": "pll_arb_cache",
       "ACON": "soccer_africa_cup_of_nations_arb_cache",
       "COPA": "soccer_africa_cup_of_nations_arb_cache",
       "EPL": "soccer_epl_arb_cache",
       "BUNDE": "soccer_germany_bundesliga_arb_cache",
       "SERIEA": "soccer_italy_serie_a_arb_cache",
       "LALIGA": "soccer_spain_la_liga_arb_cache",
       "UEFACHAMPS": "soccer_uefa_champs_league_arb_cache",
       "UEFACHAMPSQ": "soccer_uefa_champs_league_qualification_arb_cache",
       "UEFAEUROPA": "soccer_uefa_europa_league_arb_cache",
       "MLS": "soccer_usa_mls_arb_cache",
       "EUROS": "soccer_uefa_european_championship_arb_cache",
       "AUSOPENMENS1": "tennis_atp_aus_open_singles_arb_cache",
       "USOPENMENS1": "tennis_atp_us_open_arb_cache",
       "WIMBLEDONMENS1": "tennis_atp_wimbledon_arb_cache",
       "USOPENWOMENS1": "tennis_wta_us_open_arb_cache",
       "FRENCHOPENMENS1": "tennis_atp_french_open_arb_cache",
       "WIMBLEDONWOMENS1": "tennis_wta_wimbledon_arb_cache",
       "MMA": "mma_mixed_martial_arts_arb_cache",
       "NFL": "americanfootball_nfl_arb_cache",
       "CFB": "americanfootball_ncaaf_arb_cache"
    }


      self.market_view_cache_paths = {
       "NBA": "nba_market_view_cache",
       "MLB": "mlb_market_view_cache",
       "NHL": "nhl_market_view_cache",
       "NCAAB": "ncaab_market_view_cache",
       "WNBA": "wnba_market_view_cache",
       "PLL": "pll_market_view_cache",
       "ACON": "soccer_africa_cup_of_nations_market_view_cache",
       "COPA": "soccer_conmebol_copa_america_market_view_cache",
       "EPL": "soccer_epl_market_view_cache",
       "BUNDE": "soccer_germany_bundesliga_market_view_cache",
       "SERIEA": "soccer_italy_serie_a_market_view_cache",
       "LALIGA": "soccer_spain_la_liga_market_view_cache",
       "UEFACHAMPS": "soccer_uefa_champs_league_market_view_cache",
       "UEFACHAMPSQ": "soccer_uefa_champs_league_qualification_market_view_cache",
       "UEFAEUROPA": "soccer_uefa_europa_league_market_view_cache",
       "MLS": "soccer_usa_mls_market_view_cache",
       "EUROS": "soccer_uefa_european_championship_market_view_cache",
       "AUSOPENMENS1": "tennis_atp_aus_open_singles_market_view_cache",
       "USOPENMENS1": "tennis_atp_us_open_market_view_cache",
       "WIMBLEDONMENS1": "tennis_atp_wimbledon_market_view_cache",
       "USOPENWOMENS1": "tennis_wta_us_open_market_view_cache",
       "FRENCHOPENMENS1": "tennis_atp_french_open_market_view_cache",
       "WIMBLEDONWOMENS1": "tennis_wta_wimbledon_market_view_cache",
       "MMA": "mma_mixed_martial_arts_market_view_cache",
       "NFL": "americanfootball_nfl_market_view_cache",
       "CFB": "americanfootball_ncaaf_market_view_cache"

    }

  
   def get_game_ids_for_sport(self):
      directory = 'sport_game_id_files'
      
      file_path = os.path.join(directory, f'{self.sport}.json')

      with open(file_path, 'r') as file:
        data = json.load(file)
      
      return list(data.keys())
   

   def run_ev(self):

        initial_df = read_cached_df(self.ev_cache_paths[self.sport])

        new_combined_df = pd.DataFrame()

        for game_id in self.game_ids:
          
          try:
            df = read_cached_df(f"pos_ev_{game_id}")

            new_combined_df = pd.concat([new_combined_df, df], ignore_index=True)

            print(len(new_combined_df))
          except FileNotFoundError as e:
             print(e)

        print()
        print("Checked all the dfs")
        print()

        if not are_dataframes_equal(initial_df, new_combined_df):

          update_dataframe_and_cache(dataframe=new_combined_df, cache_path=self.ev_cache_paths[self.sport])

        return


   def run_arb(self):

        initial_df = read_cached_df(self.arb_cache_paths[self.sport])

        new_combined_df = pd.DataFrame()


        for game_id in self.game_ids:
          
          try:
            df = read_cached_df(f"arb_{game_id}")

            new_combined_df = pd.concat([new_combined_df, df], ignore_index=True)

            print(len(new_combined_df))
          except FileNotFoundError as e:
             print(e)

        print()
        print("Checked all the dfs")
        print()

        if not are_dataframes_equal(initial_df, new_combined_df):

          update_dataframe_and_cache(dataframe=new_combined_df, cache_path=self.arb_cache_paths[self.sport])

        return


   def run_market_view(self):

        initial_df = read_cached_df(self.market_view_cache_paths[self.sport])

        new_combined_df = pd.DataFrame()

        for game_id in self.game_ids:
          
          try:
            df = read_cached_df(f"market_view_{game_id}")

            new_combined_df = pd.concat([new_combined_df, df], ignore_index=True)

            print(len(new_combined_df))
          except FileNotFoundError as e:
             print(e)

        print()
        print("Checked all the dfs")
        print()

        if not are_dataframes_equal(initial_df, new_combined_df):

          update_dataframe_and_cache(dataframe=new_combined_df, cache_path=self.market_view_cache_paths[self.sport])

        return

if __name__ == "__main__":
   
  cfb_cacher = SportCacher("CFB")
  # nfl_cacher = SportCacher("NFL")
  mlb_cacher = SportCacher("MLB")
  wnba_cacher = SportCacher("WNBA")
  epl_cacher = SportCacher("EPL")
  mls_cacher = SportCacher("MLS")
  usopenmens1_cacher = SportCacher("USOPENMENS1")
  usopenwomens1_cacher = SportCacher("USOPENWOMENS1")
  mma_mixed_martial_arts_cacher = SportCacher("MMA")

  while True:

    cfb_cacher.run_ev()
    # nfl_cacher.run_ev()
    mlb_cacher.run_ev()
    wnba_cacher.run_ev()
    epl_cacher.run_ev()
    mls_cacher.run_ev()
    usopenmens1_cacher.run_ev()
    usopenwomens1_cacher.run_ev()
    mma_mixed_martial_arts_cacher.run_ev()

    cfb_cacher.run_arb()
    # nfl_cacher.run_arb()
    mlb_cacher.run_arb()
    wnba_cacher.run_arb()
    epl_cacher.run_arb()
    mls_cacher.run_arb()
    usopenmens1_cacher.run_arb()
    usopenwomens1_cacher.run_arb()
    mma_mixed_martial_arts_cacher.run_arb()


    cfb_cacher.run_market_view()
    # nfl_cacher.run_market_view()
    mlb_cacher.run_market_view()
    wnba_cacher.run_market_view()
    epl_cacher.run_market_view()
    mls_cacher.run_market_view()
    usopenmens1_cacher.run_market_view()
    usopenwomens1_cacher.run_market_view()
    mma_mixed_martial_arts_cacher.run_market_view()

