import requests
import pandas as pd
import json
from typing import List, Dict, Optional
from db_manager import DBManager
from typing import List, Dict, Optional, Tuple
from sqlalchemy import create_engine, select, insert, MetaData, Table, and_, func, text
from sqlalchemy.exc import SQLAlchemyError  
import unicodedata
import re
from thefuzz import fuzz

from models import LoginInfo, MMAEvents, MMAOdds, MMAGames

class PrizePicksApiCaller:
    """Class to handle PrizePicks API calls and data processing"""
    
    def __init__(self):
        self.url = 'https://partner-api.prizepicks.com/projections'
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

    def fetch_data(self) -> bool:
        """Fetch data from PrizePicks API"""
        try:
            response = requests.get(self.url, headers=self.headers)
            response.raise_for_status()
            self.data = response.json()
            return True
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return False

    def filter_projections(self, league_ids: List[str] = ['10', '12']) -> None:
        """Filter projections based on league IDs"""
        if not self.data:
            raise ValueError("No data available. Call fetch_data first.")
        
        self.filtered_data = [
            projection for projection in self.data['data']
            if projection['relationships']['league']['data']['id'] in league_ids
        ]

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
        
        # Convert the filtered data to a DataFrame
        df = pd.json_normalize(self.filtered_data)
        rows = []
        return_pp_df = pd.DataFrame(columns=[
            'market', 'BetOnline', 'Bovada', 'Jazz', 'MyBookie', 
            'BetAnySports', 'BetUS', 'DraftKings', 'FanDuel', 
            'Pinnacle', 'Betway', 'BetRivers', 'BetMGM', 
            'Caesars', 'SXBet', 'Cloudbet', 'prizepicks', 'class_name', 
            'fight_name', 'game_date', 'pulled_id', 
            'matchup', 'home_team', 'away_team', 
            'game_id', 'event_id'
        ])

        self.unique_name_df = self.get_unique_name_combinations()
        df.to_csv('pp_df.csv', index=False)


        for index, row in df.iterrows():
            line_score = float(row['attributes.line_score'])
            stat_display_name = row['attributes.stat_display_name']
            player_name = row['attributes.description']


            game_id, game_date, matched_name, other_name, event_id, my_event_id = self.get_fighters_info_from_db(row['attributes.description'])
            if game_id is None:
                continue
            #making the market names
              
            if stat_display_name == 'Fight Time (Mins)':
                market_over = f'Over {line_score / 5} rounds'
                market_under = f'Under {line_score / 5} rounds'
            else:
         
                market_over = f'{other_name} {stat_display_name} Over {line_score}'
                market_under = f'{other_name} {stat_display_name} Under {line_score}'
               
            market_over  = market_over.replace(player_name, matched_name)
            market_under = market_under.replace(player_name, matched_name)


            rows.append({
                'market': market_over,
                'class_name': 'pp',
                'BetOnline': None, 
                'Bovada': None,
                'Jazz': None,
                'MyBookie': None,
                'BetAnySports': None,
                'BetUS': None,
                'DraftKings': None,
                'FanDuel': None,
                'Pinnacle': None,
                'Betway': None,
                'BetRivers': None,
                'BetMGM': None,
                'Caesars': None,
                'SXBet': None,
                'Cloudbet': None,
                'prizepicks': '-119',
                'fight_name': my_event_id,
                'game_date': game_date,
                'pulled_id': None,
                'matchup': None,
                'home_team': other_name,
                'away_team': matched_name,
                'game_id': game_id,
                'event_id': event_id
            })
            
            rows.append({
                'market': market_under,
                'class_name': 'pp',
                'BetOnline': None,  # or '' for empty string
                'Bovada': None,
                'Jazz': None,
                'MyBookie': None,
                'BetAnySports': None,
                'BetUS': None,
                'DraftKings': None,
                'FanDuel': None,
                'Pinnacle': None,
                'Betway': None,
                'BetRivers': None,
                'BetMGM': None,
                'Caesars': None,
                'SXBet': None,
                'Cloudbet': None,
                'prizepicks': '-119',
                'fight_name': my_event_id,
                'game_date': game_date,
                'pulled_id': None,
                'matchup': None,
                'home_team': other_name,
                'away_team': matched_name,
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
        fighter_name = bet_title.split(' ')[0:2]  # Take first two words as potential name
        fighter_name = ' '.join(fighter_name)

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
    
        for _, row in self.unique_name_df.iterrows():
        
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
                best_match['my_event_id']
            )
        print(f'No match found for {bet_title}')
        return None, None, None, None, None, None



    def run(self, league_ids: List[str] = ['10', '12']) -> pd.DataFrame:
        """Execute the complete workflow"""
        if self.fetch_data():
            # self.save_response_to_file()
            self.filter_projections(league_ids)
            df = self.clean_data()
            return df

# Example usage
if __name__ == "__main__":
    caller = PrizePicksApiCaller()
    df = caller.run()
    df.to_csv('filtered_projections_pp.csv', index=False)
    existing_df = pd.read_csv('/Users/michaelblackburn/Desktop/test_new_smrt/hof_api_service/merged_output.csv')
    ### Right merge the two dataframes on the market column
    merged_df = pd.merge(existing_df, df, on='market', how='outer')
    common_columns = [col.replace('_x', '') for col in merged_df.columns if col.endswith('_x')]

    # Combine _x and _y columns
    for col in common_columns:
        # Coalesce the columns (take first non-null value)
        merged_df[col] = merged_df[f'{col}_x'].combine_first(merged_df[f'{col}_y'])
        # Drop the _x and _y columns
        merged_df = merged_df.drop([f'{col}_x', f'{col}_y'], axis=1)

    # Save the result
    merged_df.to_csv('merged_output_pp.csv', index=False)