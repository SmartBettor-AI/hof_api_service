import pandas as pd
import requests
import os
from model_db_manager import DBManager
import warnings
import time
warnings.filterwarnings("ignore")

class result_updater():
    
  def __init__ (self):
      self.db_manager = DBManager()
      self.API_KEY = os.environ.get("THE_ODDS_API_KEY")
      self.MARKETS = 'h2h'
      self.REGIONS = 'us,eu,uk'
      self.ODDS_FORMAT = 'decimal'
      self.DATE_FORMAT = 'iso'
  
  def pull_scores(self, sport):
    self.scores_df = ''
    odds_response = requests.get(
      f'https://api.the-odds-api.com/v4/sports/{sport}/scores/',
      params={
          'api_key': self.API_KEY,
          'daysFrom': int(3)
      }
    )

    if odds_response.status_code != 200:
      print(f'Failed to get odds: status_code {odds_response.status_code}, response body {odds_response.text}')

    else:
      odds_json = odds_response.json()

      df = pd.DataFrame.from_dict(odds_json)

      self.scores_df = df

      self.scores_json = odds_json

      return odds_json

  def update_results(self, sport):
     
     def get_final_score(team_name, data):
        for each in data['scores']:
           if each['name'] == team_name:
              return int(each['score'])
           else:
              pass
     
     try:
      scores_dict = self.pull_scores(sport)

      try:
          session = self.db_manager.create_session()
          df = pd.read_sql_table('scores', con=self.db_manager.get_engine())
      except Exception as e:
        print(e)
        raise
      finally:
        session.close()
      append_df = pd.DataFrame()
      for each in scores_dict:
            if each['completed'] == False:
                pass
            elif each['completed'] == True:
                home_score = get_final_score(each['home_team'], each)
                away_score = get_final_score(each['away_team'], each)
                row_data = {
                      'game_id': [each['id']],
                      'sport_title': [each['sport_title']],
                      'commence_time': [each['commence_time']],
                      'home_team': [each['home_team']],
                      'away_team': [each['away_team']],
                      'home_team_score': [home_score],
                      'away_team_score': [away_score]
                }
                row_data = pd.DataFrame(row_data)
                if home_score > away_score:
                  row_data['winning_team'] = each['home_team']
                elif home_score < away_score:
                  row_data['winning_team'] = each['away_team']
        
                # Append the dictionary data to the DataFrame
                append_df = pd.concat([append_df, row_data], ignore_index=True)
          
      new_games_df = append_df[~append_df['game_id'].isin(df['game_id'])]

      try:
        session = self.db_manager.create_session()
        new_games_df.to_sql('scores', con=self.db_manager.get_engine(), if_exists='append', index=False)
      except Exception as e:
          print(e)
      finally:
        print("great success")
        return True
     except Exception as e:
        print(e)
        print("Live results couldn't be updated. Trying agiain in 5 min... ")
        return False

if __name__ == "__main__":
   
   result_updater_instance = result_updater()

   while True:
      try:

        result_updater_instance.update_results('basketball_nba')

        result_updater_instance.update_results('icehockey_nhl')

        result_updater_instance.update_results('baseball_mlb')

        print("Successfully updated results, waiting 60 seconds to do so again...")

      except Exception as e:
         print(e)
      
      time.sleep(60)