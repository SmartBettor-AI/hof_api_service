
import requests
import pandas as pd
import json
from typing import List, Dict, Optional
from db_manager import DBManager
from typing import List, Dict, Optional, Tuple
from sqlalchemy import create_engine, select, insert, MetaData, Table, and_, or_, func, text
from sqlalchemy.exc import SQLAlchemyError  
from thefuzz import fuzz
import unicodedata
import re

from models import LoginInfo, MMAEvents, MMAOdds, MMAGames

class UnderdogApiCaller:
    '''Class to handle Underdog Fantasy API calls and data processing'''
    def __init__(self):
        self.url = "https://api.underdogfantasy.com/v2/pickem_search/search_results"
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        self.data: Optional[Dict] = None
        self.filtered_data: List[Dict] = []
        self.db_manager = DBManager()
        metadata = MetaData(bind=self.db_manager.get_engine())
        self.mma_games = Table('mma_games', metadata, autoload_with=self.db_manager.get_engine())
        self.mma_odds = Table('mma_odds_recent', metadata, autoload_with=self.db_manager.get_engine())
        self.mma_events = Table('mma_events', metadata, autoload_with=self.db_manager.get_engine())
        self.unique_name_df = pd.DataFrame()

    def fetch_data(self) -> bool:
        """Fetch data from PrizePicks API"""
        params = {"sport_id": "MMA"}
        try:
            response = requests.get(self.url, params=params)
            response.raise_for_status()
            data = response.json()
            data_to_parse = data['over_under_lines']
            self.filtered_data = data_to_parse
            
            return True
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return False

    def save_response_to_file(self, filename: str = 'response.json') -> None:
        """Save the entire JSON response to a file"""
        if self.data:
            with open(filename, 'w') as json_file:
                json.dump(self.data, json_file, indent=4)  # Save with pretty formatting
        else:
            print("No data to save.")

    def clean_data(self, filename: str = 'filtered_projections.csv') -> None:
        """Convert filtered data to a DataFrame and save to CSV"""
        if not self.filtered_data:
            print("No filtered data available. Call filter_projections first.")
            return
        

        flattened_output = []
        for i in self.filtered_data:
            for count in range(len(i['options'])):
                row_dict = {}
                row_dict['over_under'] = i['options'][count]['choice_display']
                payout_multiplier = i['options'][count]['payout_multiplier']
                row_dict['payout_multiplier'] = payout_multiplier
                decimal_odds = (20 * float(payout_multiplier)) / (1.82 * 1.82 * 1.82 * 1.82)
                row_dict['decimal_odds'] = decimal_odds
                 # Convert decimal odds to American odds
                if decimal_odds >= 2.00:
                    american_odds = round((decimal_odds - 1) * 100)
                else:
                    american_odds = round(-100 / (decimal_odds - 1))
                row_dict['american_odds'] = american_odds
                row_dict['stat'] = i['over_under']['appearance_stat']['display_stat']
                row_dict['title'] = i['over_under']['title']
                row_dict['stat_value'] = i['stat_value']
                flattened_output.append(row_dict)

        df = pd.DataFrame(flattened_output)
        df.to_csv('underdog_data.csv', index=False)
        rows = []
        return_pp_df = pd.DataFrame(columns=[
            'market', 'DraftKings', 'FanDuel', 
            'BetRivers', 'BetMGM', 
            'Caesars', 'underdog', 'class_name', 
            'fight_name', 'game_date', 'pulled_id', 
            'matchup', 'home_team', 'away_team', 
            'game_id', 'event_id'
        ])

        self.unique_name_df = self.get_unique_name_combinations()
        self.unique_name_df = self.unique_name_df.dropna(subset=['favored_team', 'underdog_team'])
        self.unique_name_df.to_csv('unique_name_df.csv', index=False)
       
        for index, row in df.iterrows():
            line_score = float(row['stat_value'])
            over_under = row['over_under']
            stat_display_name = row['stat']
            bet_title = row['title']


            game_id, game_date, matched_name, other_name, event_id, my_event_id, fighter_name_parsed = self.get_fighters_info_from_db(bet_title)

            
            if game_id is None:
                continue
            #making the market names
            matched_last_name = matched_name.split(' ')[-1]
            if stat_display_name == 'Fight Time (Mins)':
                if over_under == 'Higher':
                    if str(line_score).endswith('9'):
                        market = f'Fight goes the distance'
                    else:
                        market = f'Over {line_score / 5} rounds'
                else:
                    if str(line_score).endswith('9'):
                        market = f'Fight ends inside distance'
                    else:   
                        market = f'Under {line_score / 5} rounds'
            elif stat_display_name == 'Knockouts':
                if over_under == 'Higher':
                    market = f'{matched_last_name} wins by TKO/KO'
                else:
                    market = f"{matched_last_name} doesn't win by TKO/KO"
            elif stat_display_name == 'Submissions':
                if over_under == 'Higher':
                    market = f'{matched_last_name} wins by submission'
                else:
                    market = f"{matched_last_name} doesn't win by submission"

            elif stat_display_name == 'Finishes':
                if over_under == 'Higher':
                    market = f'{matched_name}'
                else:
                    market = f"{matched_name}"

            elif stat_display_name == '1st Round Finish':
                if over_under == 'Higher':
                    market = f'{matched_last_name} wins in round 1'
                else:
                    market = f"{matched_last_name} doesn't win in round 1"
            elif stat_display_name == '2nd Round Finish':
                if over_under == 'Higher':
                    market = f'{matched_last_name} wins in round 2'
                else:
                    market = f"{matched_last_name} doesn't win in round 2"
            elif stat_display_name == '3rd Round Finish':
                if over_under == 'Higher':
                    market = f'{matched_last_name} wins in round 3'
                else:
                    market = f"{matched_last_name} doesn't win in round 3"
            elif stat_display_name == '4th Round Finish':
                if over_under == 'Higher':
                    market = f'{matched_last_name} wins in round 4'
                else:
                    market = f"{matched_last_name} doesn't win in round 4"
            elif stat_display_name == '5th Round Finish':
                if over_under == 'Higher':
                    market = f'{matched_last_name} wins in round 5'
                else:
                    market = f"{matched_last_name} doesn't win in round 5"

            else:
                if over_under == 'Higher':
                    market = bet_title.replace(' O/U', '').replace('Points', 'Score') + ' Over ' + str(line_score)
                else:
                    market = bet_title.replace(' O/U', '').replace('Points', 'Score') + ' Under ' + str(line_score)


            #Replace any instance of the parsed fighter name with the actual fighter name
            market = market.replace(fighter_name_parsed, matched_name)
            
            
            rows.append({
                'market': market,
                'class_name': 'pp',
                
                'DraftKings': None,
                'FanDuel': None,
                
                'BetRivers': None,
                'BetMGM': None,
                'Caesars': None,
                
                'underdog': row['american_odds'],
                'fight_name': my_event_id,
                'game_date': game_date,
                'pulled_id': None,
                'matchup': None,
                'home_team': matched_name,
                'away_team': other_name,
                'game_id': game_id,
                'event_id': event_id
            })

        return_pp_df = pd.DataFrame(rows)

        drop_duplicate_df = return_pp_df.drop_duplicates(subset=['market', 'class_name', 'game_date', 'fight_name', 'home_team', 'away_team', 'game_id', 'event_id'])
        return drop_duplicate_df
    

    def get_unique_name_combinations(self) -> pd.DataFrame:
        """Get unique fighter combinations from mma_odds_recent table with game and event IDs"""
        session = self.db_manager.create_session()
        
        try:
            yesterday = pd.Timestamp.now().normalize() - pd.Timedelta(days=1)
            # Modified query to eliminate duplicates
            stmt = select(
                self.mma_odds.c.favored_team,
                self.mma_odds.c.underdog_team,
                func.min(self.mma_odds.c.id).label('id'),  # Get the minimum ID for each group
                self.mma_odds.c.game_id,
                self.mma_games.c.event_id,
                self.mma_games.c.my_game_id,
                self.mma_events.c.my_event_id,
                self.mma_odds.c.game_date
            ).join(
                self.mma_games,
                self.mma_odds.c.game_id == self.mma_games.c.id
            ).join(
                self.mma_events,
                self.mma_games.c.event_id == self.mma_events.c.id
            ).where(
                self.mma_odds.c.game_date > yesterday
            ).group_by(
                self.mma_odds.c.favored_team,
                self.mma_odds.c.underdog_team,
                self.mma_odds.c.game_id,
                self.mma_games.c.event_id,
                self.mma_games.c.my_game_id,
                self.mma_events.c.my_event_id
            )
            
            try:
                result = session.execute(stmt)
                df = pd.DataFrame(result.fetchall(), columns=[
                    'favored_team', 
                    'underdog_team', 
                    'id', 
                    'game_id', 
                    'event_id',
                    'my_game_id',
                    'my_event_id',
                    'game_date'
                ])
                
                # Additional deduplication at DataFrame level if needed
                df = df.drop_duplicates()
                
                return df
                
            except SQLAlchemyError as e:
                print(f"Error executing query: {e}")
                return pd.DataFrame()
                
        finally:
            session.close()





    def get_fighters_info_from_db(self, bet_title: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
        """Get fighters from database using fuzzy name matching"""
        # Extract the fighter name from the beginning of bet_title
        def extract_fighter_name(title: str) -> str:
            """Extract potential fighter name from bet title"""
            # Split the title into words
            words = title.split()
            
            # Common suffixes that indicate end of name
            suffixes = ['fight', 'finishes', 'knockouts', 'submissions', 'significant', 'takedowns', '1st', '2nd', '3rd', '4th', '5th', 'fantasy']
            
            # Collect words until we hit a suffix or special character
            name_words = []
            for word in words:
                # Stop if we hit a common suffix
                if word.lower() in suffixes:
                    break
                # Stop if we hit special characters (like parentheses)
                if any(char in word for char in '()[]{}'):
                    break
                name_words.append(word)
            
            return ' '.join(name_words)
        fighter_name = extract_fighter_name(bet_title)

        def normalize_name(name: str) -> str:
            """Normalize name by removing special characters and extra spaces"""
            name = name.replace('-', ' ')
            # Remove accents and special characters
            name = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('ASCII')
            # Remove any non-letter characters except spaces
            name = re.sub(r'[^a-zA-Z\s]', '', name)
            # Convert to lowercase and remove extra spaces
            name = ' '.join(name.lower().split())
            name = name.split(' ')[0:2]
            name = ' '.join(name)
            return name
        
        # Function to calculate fuzzy match ratio
        def get_match_ratio(name1: str, name2: str) -> int:
            norm_name1 = normalize_name(name1)
            norm_name2 = normalize_name(name2)
            return fuzz.ratio(norm_name1, norm_name2)
        
        best_match = None
        best_ratio = 0
        
        # Look through both favored and underdog teams
        i = 0
        for _, row in self.unique_name_df.iterrows():
            i += 1
            favored_two_words = row['favored_team']
            favored_ratio = get_match_ratio(fighter_name, favored_two_words)
            underdog_two_words = row['underdog_team']
            underdog_ratio = get_match_ratio(fighter_name, underdog_two_words)
            
            # Use the better of the two ratios
            current_ratio = max(favored_ratio, underdog_ratio)
            
            # If this is the best match so far and the ratio is good enough
            if current_ratio > best_ratio and current_ratio > 80:  # 80% threshold
                best_ratio = current_ratio
                best_match = row
                is_favored = favored_ratio > underdog_ratio
        
        if best_match is not None:
            if is_favored:
                matched_name = best_match['favored_team']
                other_name = best_match['underdog_team']
            else:
                matched_name = best_match['underdog_team']
                other_name = best_match['favored_team']
                
            return (
                best_match['game_id'],
                best_match['game_date'],
                matched_name,
                other_name,
                best_match['event_id'],
                best_match['my_event_id'],
                fighter_name
            )
        print(f'No match found for {bet_title}')
        return None, None, None, None, None, None, None

    def run(self, league_ids: List[str] = ['10', '12']) -> pd.DataFrame:
        """Execute the complete workflow"""
        if self.fetch_data():
            df = self.clean_data()
            return df

# Example usage
if __name__ == "__main__":
    caller = UnderdogApiCaller()
    df = caller.run()
    df.to_csv('filtered_projections.csv', index=False)
    existing_df = pd.read_csv('merged_output.csv')
    ### Right merge the two dataframes on the market column
    merged_df = pd.merge(existing_df, df, on=['market', 'game_id'], how='outer')
    common_columns = [col.replace('_x', '') for col in merged_df.columns if col.endswith('_x')]

    # Combine _x and _y columns
    for col in common_columns:
        # Coalesce the columns (take first non-null value)
        merged_df[col] = merged_df[f'{col}_x'].combine_first(merged_df[f'{col}_y'])
        # Drop the _x and _y columns
        merged_df = merged_df.drop([f'{col}_x', f'{col}_y'], axis=1)

    # Save the result
    merged_df.to_csv('merged_output_pp.csv', index=False)