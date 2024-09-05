import pandas as pd
import redis
redis_client = redis.Redis(host='localhost', port=6379, db=0)

def update_dataframe_and_cache(dataframe, path):
    serialized_df = dataframe.to_json()
    redis_client.set(path, serialized_df)


if __name__ == "__main__":
  nba_ev = pd.read_csv('pos_ev_data/nba_pos_ev_data.csv')
  nhl_ev = pd.read_csv('pos_ev_data/nhl_pos_ev_data.csv')
  mlb_ev = pd.read_csv('pos_ev_data/mlb_pos_ev_data.csv')
  wnba_ev = pd.read_csv("pos_ev_data/wnba_pos_ev_data.csv")
  pll_ev = pd.read_csv("pos_ev_data/pll_pos_ev_data.csv")
  acon_ev = pd.read_csv("pos_ev_data/soccer_africa_cup_of_nations_pos_ev_data.csv")
  copa_ev = pd.read_csv("pos_ev_data/soccer_conmebol_copa_america_pos_ev_data.csv")
  epl_ev = pd.read_csv("pos_ev_data/soccer_epl_pos_ev_data.csv")
  bunde_ev = pd.read_csv("pos_ev_data/soccer_germany_bundesliga_pos_ev_data.csv")
  seriea_ev = pd.read_csv("pos_ev_data/soccer_italy_serie_a_pos_ev_data.csv")
  laliga_ev = pd.read_csv("pos_ev_data/soccer_spain_la_liga_pos_ev_data.csv")
  uefachamps_ev = pd.read_csv("pos_ev_data/soccer_uefa_champs_league_pos_ev_data.csv")
  uefachampsq_ev = pd.read_csv("pos_ev_data/soccer_uefa_champs_league_qualification_pos_ev_data.csv")
  uefaeuropa_ev = pd.read_csv("pos_ev_data/soccer_uefa_europa_league_pos_ev_data.csv")
  mls_ev = pd.read_csv("pos_ev_data/soccer_usa_mls_pos_ev_data.csv")
  euros_ev = pd.read_csv("pos_ev_data/soccer_uefa_european_championship_pos_ev_data.csv")
  ausopenmens1_ev = pd.read_csv("pos_ev_data/tennis_atp_aus_open_singles_pos_ev_data.csv")
  usopenmens1_ev = pd.read_csv("pos_ev_data/tennis_atp_us_open_pos_ev_data.csv")
  wimbledonmens1_ev = pd.read_csv("pos_ev_data/tennis_atp_wimbledon_pos_ev_data.csv")
  usopenwomens1_ev = pd.read_csv("pos_ev_data/tennis_wta_us_open_pos_ev_data.csv")
  frenchopenmens1_ev = pd.read_csv("pos_ev_data/tennis_atp_french_open_pos_ev_data.csv")
  wimbledonwomens1_ev = pd.read_csv("pos_ev_data/tennis_wta_wimbledon_pos_ev_data.csv")
  mma_ev = pd.read_csv("pos_ev_data/mma_mixed_martial_arts_pos_ev_data.csv")
  nfl_ev = pd.read_csv("pos_ev_data/americanfootball_nfl_pos_ev_data.csv")
  ncaaf_ev = pd.read_csv("pos_ev_data/americanfootball_ncaaf_pos_ev_data.csv")


  pos_ev_dash_cache = pd.concat([nba_ev, nhl_ev, mlb_ev, wnba_ev, pll_ev, acon_ev, copa_ev, epl_ev, bunde_ev, seriea_ev, laliga_ev, uefachamps_ev, uefachampsq_ev, uefaeuropa_ev, mls_ev, euros_ev, ausopenmens1_ev, usopenmens1_ev, wimbledonmens1_ev, usopenwomens1_ev, frenchopenmens1_ev, wimbledonwomens1_ev, mma_ev, nfl_ev, ncaaf_ev], ignore_index=True)



  update_dataframe_and_cache(nba_ev, 'nba_pos_ev_cache')
  update_dataframe_and_cache(nhl_ev, 'nhl_pos_ev_cache')
  update_dataframe_and_cache(mlb_ev, 'mlb_pos_ev_cache')
  update_dataframe_and_cache(wnba_ev, 'wnba_pos_ev_cache')  
  update_dataframe_and_cache(pll_ev, 'pll_pos_ev_cache')
  update_dataframe_and_cache(acon_ev,'soccer_africa_cup_of_nations_pos_ev_cache')
  update_dataframe_and_cache(copa_ev,'soccer_conmebol_copa_america_pos_ev_cache')
  update_dataframe_and_cache(epl_ev,'soccer_epl_pos_ev_cache')
  update_dataframe_and_cache(bunde_ev,'soccer_germany_bundesliga_pos_ev_cache')
  update_dataframe_and_cache(seriea_ev,'soccer_italy_serie_a_pos_ev_cache')
  update_dataframe_and_cache(laliga_ev,'soccer_spain_la_liga_pos_ev_cache')
  update_dataframe_and_cache(uefachamps_ev,'soccer_uefa_champs_league_pos_ev_cache')
  update_dataframe_and_cache(uefachampsq_ev,'soccer_uefa_champs_league_qualification_pos_ev_cache')
  update_dataframe_and_cache(uefaeuropa_ev,'soccer_uefa_europa_league_pos_ev_cache')
  update_dataframe_and_cache(mls_ev,'soccer_usa_mls_pos_ev_cache')
  update_dataframe_and_cache(euros_ev,'soccer_uefa_european_championship_pos_ev_cache')
  update_dataframe_and_cache(ausopenmens1_ev, 'tennis_atp_aus_open_singles_pos_ev_cache')
  update_dataframe_and_cache(usopenmens1_ev, 'tennis_atp_us_open_pos_ev_cache')
  update_dataframe_and_cache(wimbledonmens1_ev, 'tennis_atp_wimbledon_pos_ev_cache')
  update_dataframe_and_cache(usopenwomens1_ev, 'tennis_wta_us_open_pos_ev_cache')
  update_dataframe_and_cache(frenchopenmens1_ev, 'tennis_atp_french_open_pos_ev_cache')
  update_dataframe_and_cache(wimbledonwomens1_ev, 'tennis_wta_wimbledon_pos_ev_cache')
  update_dataframe_and_cache(mma_ev,'mma_mixed_martial_arts_pos_ev_cache')
  update_dataframe_and_cache(nfl_ev, 'americanfootball_nfl_pos_ev_cache')
  update_dataframe_and_cache(ncaaf_ev, 'americanfootball_ncaaf_pos_ev_cache')
  

  update_dataframe_and_cache(pos_ev_dash_cache, 'pos_ev_dash_cache')
  print()
  print("All +EV DataFrames re-cached!")
  print()