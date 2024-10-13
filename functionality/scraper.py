import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, ElementClickInterceptedException

from bs4 import BeautifulSoup
from selenium.webdriver.chrome.options import Options
from db_manager import DBManager
from models import LoginInfo, MMAEvents, MMAOdds, MMAGames
from sqlalchemy import create_engine, select, insert, MetaData, Table, and_
from sqlalchemy.orm import aliased

import pandas as pd
import jsonpickle
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
import re
import json
import numpy as np
import time
import random
import http.client
import os
from datetime import datetime, timedelta
import uuid
import redis
from sqlalchemy import desc, func, select
redis_client = redis.Redis(host='localhost', port=6379, db=0)

class MMAScraper:

    def __init__(self, url):
        self.options = Options()
        # configure the profile to store cookies and cache
        self.db_manager = DBManager()
        metadata = MetaData(bind=self.db_manager.get_engine())
        self.mma_games = Table('mma_games', metadata, autoload_with=self.db_manager.get_engine())
        self.mma_odds = Table('mma_odds', metadata, autoload_with=self.db_manager.get_engine())
        self.mma_events = Table('mma_events', metadata, autoload_with=self.db_manager.get_engine())

        self.options.add_argument("--use-fake-ui-for-media-stream")
        self.options.add_argument("--use-fake-device-for-media-stream")
        self.options.add_argument("--disable-extensions")
        self.options.add_argument("--headless")
        self.options.add_argument("--disable-dev-shm-usage")
        self.options.add_argument("--no-sandbox")
        self.options.add_argument("--disable-popup-blocking")
        self.options.add_argument("--disable-gpu")  # Disable GPU for headless mode
        self.options.add_argument("start-maximized")
        self.options.add_argument("disable-infobars")
        self.options.add_argument("--disable-web-security")
        self.options.add_argument("--disable-notifications")
        self.options.add_argument("--enable-automation")
        self.options.add_argument("--disable-background-timer-throttling")
        self.options.add_argument("--disable-backgrounding-occluded-windows")
        self.options.add_argument("--disable-breakpad")
        self.options.add_argument("--disable-client-side-phishing-detection")
        self.options.add_argument("--disable-default-apps")
        self.options.add_argument("--disable-hang-monitor")
        self.options.add_argument("--disable-popup-blocking")
        self.options.add_argument("--disable-prompt-on-repost")
        self.options.add_argument("--disable-sync")
        self.options.add_argument("--disable-translate")
        self.options.add_argument("--metrics-recording-only")
        self.options.add_argument("--safebrowsing-disable-auto-update")
        self.options.add_argument("--enable-automation")
        self.options.add_argument("--password-store=basic")
        self.options.add_argument("--use-mock-keychain")
        self.url = url
        self.driver_path = os.environ.get("chromedriver_path")
        # self.driver = webdriver.Chrome(service=Service(self.driver_path),options=options)  # Initialize the driver here
        # self.soup = self._get_soup()

    def _get_soup(self):

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(self.url, headers=headers)
        print(response.text)

        # Parse the page content with BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')
        return soup
        
           # Ensure the driver quits after use
    

class BestFightOddsScraper(MMAScraper):
    def scrape_event_data(self, i):  

    # Headers to mimic the browser request  
        # Ant
        api_key_1 = os.environ.get("scraping_ant_key1")
        api_key_2 = os.environ.get("scraping_ant_key2")
        api_key_3 = os.environ.get("scraping_ant_key3")
        api_key_4 = os.environ.get("scraping_ant_key4")
        api_key_5 = os.environ.get("scraping_ant_key5")
        api_key_6 = os.environ.get("scraping_ant_ke6")
        api_key_7 = os.environ.get("scraping_ant_key7")
        api_key_8 = os.environ.get("scraping_ant_key8")
        api_key_9 = os.environ.get("scraping_ant_key9")
        api_key_10 = os.environ.get("scraping_ant_key10")
        

        api_keys = [api_key_10, api_key_9, api_key_8, api_key_5, api_key_6, api_key_7, api_key_3, api_key_4, api_key_2, api_key_1]
        

        retries = 0
        max_retries = 5
        while retries < max_retries:
            try:
                api_key =api_keys[i % len(api_keys)]
                logger.info(i)
                conn = http.client.HTTPSConnection("api.scrapingant.com")
                conn.request("GET", f"/v2/general?url={self.url}&x-api-key={api_key}")
                
                res = conn.getresponse()
                if res.status != 200:
                    logger.info(f'Response failed id: {api_key}')
                    logger.info(res.read())
                    raise Exception(f"Request failed with status code {res.status}")
                    
                data = res.read()
                logger.info('Received response')
                # print(data)
                
                html_content = data.decode("utf-8")
                soup = BeautifulSoup(html_content, "html.parser")
                break
                
            except Exception as e:
                logger.info(f"Request failed: {e}")
                retries += 1
                if retries < max_retries:
                    # Set a random delay between 1 and 5 seconds
                    delay = random.uniform(1, 5)
                    print(f"Retrying in {delay:.2f} seconds...")
                    time.sleep(delay)
                    i += 1
                else:
                    logger.info("Max retries reached. Exiting.")
                    raise


        table_divs = soup.find_all("div", class_="table-div")

        # Process the extracted divs as needed
        for div in table_divs:
            h1_tag = div.find("h1")
            if "Future" in h1_tag.text:
                continue
            span_tag = div.find('span')
            if h1_tag:
                print(h1_tag.text)
                print(span_tag.text)
            tables = div.find_all("table")
            if len(tables) >= 2:
                # Get the second table
                second_table = tables[1]

                # Extract the class names of each <tr> and store them
                rows = second_table.find_all("tr")
                class_names = [row.get("class", [""])[0] for row in rows]  # Get the class or an empty string if none
                # Exclude the class name of the first row
                class_names = class_names[1:]

                # Convert the table to a DataFrame
                df = pd.read_html(str(second_table))[0]
                df = df.rename(columns={'Unnamed: 0': 'market'})
                df = df.drop(columns=['Props', 'Props.1', 'Props.2'])


                patterns_to_remove = r'\b(Jr\.|jr\.|Jr|jr|Sr\.|sr\.|Sr|sr|III|II)\b\.?'

                # Apply the removal to all string columns in the DataFrame
                df = df.replace(to_replace=patterns_to_remove, value='', regex=True)

                # Optionally, you can strip any leading/trailing whitespace
                df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
                

                # Add the class names as a new column in the DataFrame
                df['class_name'] = class_names

                columns_to_check = df.columns.difference(['market', 'class_name'])

                # Step 2: Drop rows where all specified columns are NaN
                df = df.dropna(subset=columns_to_check, how='all')
                df = df[df['market'] != "Any other result"]

                df.replace('-100000.0', np.nan, inplace=True)
                df.replace(-100000.0, np.nan, inplace=True)
                df.replace(-100000, np.nan, inplace=True)
                df.replace('-100000', np.nan, inplace=True)

                
                df.to_csv('with_bad_rows.csv', index=False)

                df['fight_name'] = h1_tag.text
                date_str = span_tag.text
                parts = date_str.split()
                date_str = parts[0] + ' ' + parts[1].replace('st', '').replace('nd', '').replace('rd', '').replace('th', '')
                month_day = datetime.strptime(date_str, '%B %d')
                now = datetime.now()
                current_year = now.year
                current_month = now.month
                month = month_day.month
                day = month_day.day
                
                # Determine the year
                if month >= current_month:
                    year = current_year
                else:
                    year = current_year + 1
                

                final_date = datetime(year, month, day)

                # Subtract one day
                new_date = final_date - timedelta(days=1)

                # Format the new date as a string
                final_date_str = new_date.strftime('%Y_%m_%d')
                
                df['game_date'] =  final_date_str


                # Create the file path and replace spaces with underscores
                path = f'{os.getcwd()}/mma_raw_odds/{h1_tag.text}_{span_tag.text}.csv'.replace(' ', '_')

                # Save the DataFrame to CSV
                df.to_csv(path, index=False)
                print(f"Saved DataFrame to {path}")
            else:
                print("Less than two tables found in this div.")

    def format_odds(self):
        files = os.listdir(f"{os.getcwd()}/mma_raw_odds")

        file_prefix = f"{os.getcwd()}/mma_raw_odds/"

        total_df = pd.DataFrame()

        # this is for MLB and it splits into pitching and batting data, instead we're not gonna aplit it up but instead concatenate all of them together because overlapping column names can be handled now 
        for file in files:
            if file.endswith('.csv'):
                if 'Future' not in file:
                    print(f'Formatting {file}...')
                    file_name = (file_prefix+file).strip()
                    try:
                        this_df = pd.read_csv(file_name)
                        try:
                            os.remove(file_name)
                            print(f'Deleted {file_name}')
                        except OSError as e:
                            print(f"Error deleting {file_name}: {e}")
                        this_df['matchup'] = ""
                        for col in this_df.columns:
                            if this_df[col].dtype == 'object':  # Check if column contains string data
                                this_df[col] = this_df[col].apply(lambda x: self.convert_fraction_to_float(x) if isinstance(x, str) else x)


                        this_df.replace({'▼': '', '▲': ''}, regex=True, inplace=True)
        

                        current_away_fighter = ''
                        current_home_fighter = ''
                        player_count = 0
                        this_df.to_csv('before_loop.csv', index=False)
                        for idx, row in this_df.iterrows():
                            this_df.to_csv('how_is_this_working.csv', index=False)
                            if pd.isna(row['class_name']) or row['class_name'].strip() == '':
                                if player_count == 0:
                                    player_count = 1
                                    current_away_fighter = row['market']
                                elif player_count == 1:   
                                    player_count = 2
                                    current_home_fighter = row['market']

                                    if current_home_fighter < current_away_fighter:
                                        team_1 = current_home_fighter
                                        team_2 = current_away_fighter
                                    else:
                                        team_1 = current_away_fighter
                                        team_2 = current_home_fighter

                                    this_df.at[idx-1, 'matchup'] = team_1 + 'v.' + team_2
                                    this_df.at[idx, 'matchup'] = team_1 + 'v.' + team_2
                        
                                    this_df.at[idx-1, 'home_team'] = team_1
                                    this_df.at[idx, 'home_team'] = team_1
        
                                    this_df.at[idx-1, 'away_team'] = team_2
                                    this_df.at[idx, 'away_team'] = team_2

                                    my_game_id = team_1.replace(' ', '_') + '_' + team_2.replace(' ', '_') + '_' + row['game_date'].replace(' ', '_').split('_')[0]
                                    
                                    game_id, event_id = self.get_or_create_ids(my_game_id, row['fight_name'])


                                    this_df.at[idx - 1, 'game_id'] = int(game_id)
                                    this_df.at[idx, 'game_id'] = int(game_id)
                                    

                                    this_df.at[idx - 1, 'event_id'] = int(event_id)
                                    this_df.at[idx, 'event_id'] = int(event_id)

                                    
                                    player_count = 0
                            else:
                                    if str(current_home_fighter) < str(current_away_fighter):
                                        team_1 = str(current_home_fighter)
                                        team_2 = str(current_away_fighter)
                                    else:
                                        team_1 = str(current_away_fighter)
                                        team_2 = str(current_home_fighter)

                                    this_df.at[idx, 'matchup'] = team_1 + 'v.' + team_2
                                    this_df.at[idx, 'away_team'] = team_2
                                    this_df.at[idx, 'home_team'] = team_1
                                    my_game_id = team_1.replace(' ', '_') + '_' + team_2.replace(' ', '_') + '_' + row['game_date'].replace(' ', '_').split('_')[0]
                                    game_id, event_id = self.get_or_create_ids(my_game_id, row['fight_name'])
                                    this_df.at[idx, 'game_id'] = int(game_id)

                                    this_df.at[idx, 'event_id'] = int(event_id)
   
                    except Exception as e:
                        print(f"Error processing {file_name}: {e}")
                        # Delete the problematic file
                        try:
                            os.remove(file_name)
                            print(f"Deleted {file_name} due to error")
                            continue
                        except OSError as delete_error:
                            print(f"Error deleting {file_name}: {delete_error}")
                            continue


                    total_df = pd.concat([total_df, this_df])
        
        return total_df

                






    def replace_fraction(self,symbol):
        """Replace fraction symbol ½ with its decimal equivalent."""
        return symbol.replace("½", ".5")

    def convert_fraction_to_float(self,text):
        """Convert fraction symbol ½ to float while keeping other text intact."""
        # Replace fraction symbol with decimal equivalent
        text_decimal = self.replace_fraction(text)
        # Extract all numbers with decimals
        result = re.sub(r'(\d+)\.(5)', r'\1.5', text_decimal)
        return result
    def get_or_create_ids(self, my_game_id, my_event_id):
        session = self.db_manager.create_session()
        
        try:
            # Step 1: Check if the my_game_id exists in mma_games
            stmt = select(self.mma_games.c.id, self.mma_games.c.event_id).where(self.mma_games.c.my_game_id == my_game_id)
            result = session.execute(stmt).fetchone()

            if result:
                # Case 2: my_game_id exists, return the existing game id and event id
                game_id, event_id = result
                return game_id, event_id

            
            # Case 1: my_game_id does not exist, check if my_event_id exists in mma_events
            stmt = select(self.mma_events.c.id).where(self.mma_events.c.my_event_id == my_event_id)
            event_id = session.execute(stmt).scalar()

            if not event_id:
                # If my_event_id does not exist, create a new entry in mma_events
                stmt = insert(self.mma_events).values(my_event_id=my_event_id)
                event_result = session.execute(stmt)
                session.commit()
                event_id = event_result.inserted_primary_key[0]  # Get the new event id
            
            # Step 3: Create a new entry in mma_games with the fetched/created event_id
            stmt = insert(self.mma_games).values(my_game_id=my_game_id, event_id=event_id)
            game_result = session.execute(stmt)
            session.commit()
            game_id = game_result.inserted_primary_key[0]  # Get the new game id

            # Step 4: Return the new ids for both mma_games and mma_events
            return game_id, event_id
        
        finally:
            session.close()    

    def market_key_map(self, row, first_totals_over_under):
        if pd.isna(row['class_name']) or row['class_name'].strip() == '':
            first_totals_over_under[0] = False
            first_totals_over_under[0] = False
            return 'h2h'
        
        if 'Over' in row['market']:
            if first_totals_over_under[0] == False and row['highest_bettable_odds'] >= 1.4 and row['highest_bettable_odds'] <= 3.5:
                first_totals_over_under[0] = True
                return 'main_totals'
            else:
                return 'totals'
                
        elif 'Under' in row['market']:
            if first_totals_over_under[1] == False and row['highest_bettable_odds'] >= 1.4 and row['highest_bettable_odds'] <= 3.5:
                first_totals_over_under[1] = True
                return 'main_totals'
            else: 
                return 'totals'
        else:
            return ''

    def find_matching_columns(self, row, bettable_books):
        matching_cols = [col.title() for col in bettable_books if row[col] == row['highest_bettable_odds']]
        return list(set(matching_cols))


    def american_to_decimal(self, american_odds):
        if pd.isna(american_odds):
            return np.nan
        
        if isinstance(american_odds, float):
            american_odds_int = int(american_odds)
        elif isinstance(american_odds, int):
            american_odds_int = american_odds

        elif isinstance(american_odds, str):
            if american_odds.startswith('+'):
                american_odds = american_odds[1:]

            american_odds_int = int(american_odds)

        if american_odds_int >= 0:
            decimal = (american_odds_int / 100) + 1
        else:
            decimal = (100 / abs(american_odds_int)) + 1

        return decimal



class fightOddsIOScraper(MMAScraper):
    def scrape_event_data(self, i):  
    
            api_key_1 = os.environ.get("scraping_ant_key1")
            api_key_2 = os.environ.get("scraping_ant_key2")
            api_key_3 = os.environ.get("scraping_ant_key3")
            api_key_4 = os.environ.get("scraping_ant_key4")
            api_key_5 = os.environ.get("scraping_ant_key5")
            api_key_6 = os.environ.get("scraping_ant_key6")
            api_key_7 = os.environ.get("scraping_ant_key7")
            api_key_8 = os.environ.get("scraping_ant_key8")
            api_key_9 = os.environ.get("scraping_ant_key9")
            api_key_10 = os.environ.get("scraping_ant_key10")
            
            
            api_keys = [api_key_10, api_key_9, api_key_8, api_key_6, api_key_7, api_key_5, api_key_3, api_key_4, api_key_2,api_key_1]

            retries = 0
            max_retries = 5
            while retries < max_retries:
                api_key =api_keys[i % len(api_keys)]
                logger.info(i)
                try:
                    conn = http.client.HTTPSConnection("api.scrapingant.com")
                    conn.request("GET", f"/v2/general?url={self.url}&x-api-key={api_key}")
                    
                    res = conn.getresponse()
                    if res.status != 200:
                        logger.info(res.read())
                        logger.info(api_key)
                        raise Exception(f"Request failed with status code {res.status}")
                        
                    data = res.read()
                    logger.info('Received response')
                    # print(data)
                    
                    html_content = data.decode("utf-8")
                    soup = BeautifulSoup(html_content, "html.parser")
                    break
                    
                except Exception as e:
                    logger.info(f"Request failed: {e}")
                    retries += 1
                    if retries < max_retries:
                        # Set a random delay between 1 and 5 seconds
                        delay = random.uniform(1, 5)
                        print(f"Retrying in {delay:.2f} seconds...")
                        time.sleep(delay)
                        i+=1
                    else:
                        print("Max retries reached. Exiting.")
                        raise


            table_divs = soup.find_all("a", class_="MuiButtonBase-root")

            # Process the extracted divs as needed
            for div in table_divs:
                print(div.get('href'))
                full_url = self.url + div.get('href')[1:]
                if 'odds/' in full_url:
                    self.get_odds_per_page(full_url)
                    




    def get_odds_per_page(self, url):
        try:
            driver = webdriver.Chrome(service=Service(self.driver_path),options=self.options)

            driver.get(url)

            try:
                buttons = driver.find_elements(By.CSS_SELECTOR, ".MuiButtonBase-root.MuiButton-root.MuiButton-contained")
            
                if buttons:
                    # Wait until the buttons are visible and clickable
                    WebDriverWait(driver, 10).until(
                        EC.visibility_of_all_elements_located((By.CSS_SELECTOR, ".MuiButtonBase-root.MuiButton-root.MuiButton-contained"))
                    )

                    # Find all buttons and click them
                    buttons = driver.find_elements(By.CSS_SELECTOR, ".MuiButtonBase-root.MuiButton-root.MuiButton-contained")
                    for button in buttons:
                        button.click()
                        time.sleep(1)  # Optional: add a small delay between clicks

                # Get the updated page source after clicking the buttons
                html_content = driver.page_source

                # Close the WebDriver
                driver.quit()

                soup = BeautifulSoup(html_content, "html.parser")

                # Now you can find the table or any other elements you're interested in
                table = soup.find("table")

            except Exception as e:
                print(f"An error occurred: {e}")
                driver.quit()


            # Extract the class names of each <tr> and store them
            class_names = []
            rows = table.find_all("tr")
            last_btn_or_no = False
            for row in rows: 
                tds = row.find_all("td")
            
                if tds:
                # Get the last <td> element
                    last_td = tds[-1]
                    btn = last_td.find("button")
                    
                    # Check if there is a <button> tag within the last <td>
                    if btn:
                        #Case where there is an odds button in the last td (weird thing where emties are two rows so the last is an odds if there is clcoudbet)
                        style = btn.get('style', '')

                        aria_label = btn.get('aria-label')
                        #append twice to account for the previous row ml
                        if aria_label:
                            if 'account' in aria_label:
                                if not last_btn_or_no: 
                                    class_names.pop() 

                                class_names.append('')
                                class_names.append('')
                                last_btn_or_no = True
                        elif 'padding' in style:
                            class_names.append('pr')
                            last_btn_or_no = False
                        else:
                            last_btn_or_no = True
                            continue
                    else:
                        last_btn_or_no = False
                        class_names.append('pr')
            # # Exclude the class name of the first row
                        
            print(len(class_names))
            print(len)
            # class_names = class_names[1:]

            # Convert the table to a DataFrame

            df = pd.read_html(str(table))[0]
            print(len(df))
            df['class_name'] = class_names
            df.to_csv('oddstable.csv')
            df = df.rename(columns={'Fighters': 'market'})
            df = df.drop(columns=['Unnamed: 16'])

            columns_to_check = df.columns.difference(['market', 'class_name'])

            # Step 2: Drop rows where all specified columns are NaN
            df = df.dropna(subset=columns_to_check, how='all')
            # df = df.drop(columns=['Bovada'])

            name_and_date = self.find_fight_name_and_date(soup)
            df['fight_name'] = name_and_date[0]
            date_str = name_and_date[1]
            month_day = datetime.strptime(date_str, '%B %d')
            now = datetime.now()
            current_year = now.year
            current_month = now.month
            month = month_day.month
            day = month_day.day

            # Determine the year
            if month >= current_month:
                year = current_year
            else:
                year = current_year + 1

            final_date = datetime(year, month, day)

            # # add one day
            # new_date = final_date + timedelta(days=1)

            # Format the new date as a string
            final_date_str = final_date.strftime('%Y_%m_%d')
            
            df['game_date'] =  final_date_str


            # Create the file path and replace spaces with underscores
            path = f'{os.getcwd()}/mma_raw_odds/{name_and_date[0]}_{name_and_date[1]}.csv'.replace(' ', '_')

            # Save the DataFrame to CSV
            df.to_csv(path, index=False)
        except Exception as e:
            print(f"An error occurred: {e}")
            return  # Return on any error
    # Burns v Brady fight
    # Fight Lines: Moneylines, draw, main totals (grouped), fight to go the distance
    # Round props: FIghter Wins in round 1, 2, 3, inside distance 

    # Methd of victory: 
    #   Wins by KO/TKO/DQ (grouped by round)
    #   Wins by submission (grouped by round)
    #   Wins by decision (grouped by decision)
    ##############

    # Other props:
    #   Fight doesn't end in split or majority
    #   Fight ends in split or majority
    def get_mma_data_for_cache(self):
        cache_key = "mma_data"
        event_data = self.get_mma_data()
        redis_client.delete(cache_key)
        logger.info('delete the cache')
        # Store the result in Redis with a timeout (e.g., 1 hour = 3600 seconds)
        redis_client.set(cache_key, jsonpickle.encode(event_data), ex=1800)
        logger.info('wrote the cache')
        return
    
    def get_mma_game_data_for_cache(self):
        ls = self.get_unique_game_ids()
        for game_id in ls:
            game_data = self.get_MMA_game_data(game_id)
            cache_key = f"mma_game_data:{game_id}"
            redis_client.delete(cache_key)
            redis_client.set(cache_key, jsonpickle.encode(game_data), ex=1800)
        logger.info('wrote all caches')



        

    
    def get_unique_game_ids(self):
        with self.db_manager.create_session() as session:
            # SQLAlchemy query to get all unique game_ids
            stmt = (
                select(MMAOdds.game_id)
                .distinct()
            )
            
            result = session.execute(stmt)
            
            # Fetch all unique game_ids and return as a list
            unique_game_ids = [row[0] for row in result]
        
        return unique_game_ids

    def get_MMA_game_data(self, gameId):
      
      with self.db_manager.create_session() as session:
        today = func.current_date()
        one_day_ago = datetime.now() - timedelta(days=1)

        # Subquery to get the most recent pulled_time for each game_id and market
  
        latest_odds = (
            select(
                MMAOdds.game_id,
                MMAOdds.market,
                func.max(MMAOdds.pulled_time).label('max_pulled_time')
            )
            .filter(and_(
                MMAOdds.game_date >= one_day_ago,
                MMAOdds.pulled_time >= one_day_ago,
                MMAOdds.game_id == gameId
            ))
            .group_by(MMAOdds.game_id, MMAOdds.market)
            .subquery()
        )

        # Main query

        stmt = (
            select(
                MMAOdds,
                MMAGames.my_game_id,
                MMAEvents.my_event_id
            )
            .join(latest_odds, and_(
                MMAOdds.game_id == latest_odds.c.game_id,
                MMAOdds.market == latest_odds.c.market,
                MMAOdds.pulled_time == latest_odds.c.max_pulled_time
            ))
            .join(MMAGames, MMAOdds.game_id == MMAGames.id)
            .join(MMAEvents, MMAOdds.event_id == MMAEvents.id)
            .filter(MMAOdds.game_id == gameId)
        )

        result = session.execute(stmt)

        
        data = [
            {
                'id': mma_odds.id,
                'market': mma_odds.market,
                'pulled_time': str(mma_odds.pulled_time),
                'odds': mma_odds.odds,
                'highest_bettable_odds': mma_odds.highest_bettable_odds,
                'sportsbooks_used': mma_odds.sportsbooks_used,
                'market_key': mma_odds.market_key,
                'game_date': mma_odds.game_date,
                'game_id': mma_odds.game_id,
                'my_game_id': my_game_id,
                'my_event_id': my_event_id,
                'average_market_odds': mma_odds.average_market_odds,
                'market_type': mma_odds.market_type,
                'dropdown': mma_odds.dropdown,
                'favored_team': mma_odds.favored_team,
                'underdog_team': mma_odds.underdog_team
            }
            for mma_odds, my_game_id, my_event_id in result
        ]


        return data
   
    def get_mma_data(self):
      session = self.db_manager.create_session()
  
      
      try:
        today = func.current_date()
        one_day_ago = datetime.now() - timedelta(days=1)

        latest_odds = aliased(MMAOdds)
        other_side = aliased(MMAOdds)

        # Subquery to get the most recent pulled_time for each game_id and market
        subquery = (
            select(
                latest_odds.id,
                latest_odds.game_id,
                latest_odds.market,
                latest_odds.pulled_id,
                func.max(latest_odds.pulled_time).label('max_pulled_time')
            )
            .where(latest_odds.game_date >= one_day_ago, latest_odds.market_key.in_(['h2h']))
            .where(latest_odds.pulled_time >= one_day_ago)
            .group_by(latest_odds.game_id, latest_odds.market)
            .subquery()
        )

        # Main query with joins
        stmt = (
            select(
                MMAOdds,
                MMAGames.my_game_id,
                MMAEvents.my_event_id,
                other_side
            )
            .join(subquery, (MMAOdds.game_id == subquery.c.game_id) &
                  (MMAOdds.market == subquery.c.market) &
                  (MMAOdds.pulled_time == subquery.c.max_pulled_time))
            .join(MMAGames, MMAOdds.game_id == MMAGames.id)  # Join with MMAGames to get my_game_id
            .join(MMAEvents, MMAOdds.event_id == MMAEvents.id)  # Join with MMAEvents to get my_event_id
            .outerjoin(
                other_side,
                (MMAOdds.game_id == other_side.game_id) &
                (MMAOdds.market != other_side.market) &
                (MMAOdds.market_key == other_side.market_key) &
                (other_side.pulled_id == subquery.c.pulled_id) &
                (other_side.id < subquery.c.id)
            )
        )

        # Execute the optimized query
        rows = session.execute(stmt).all()

        # Process results
        data = []
        for mma_odds, my_game_id, my_event_id, other_row in rows:
            if other_row:
                row_data = {
                    'id': mma_odds.id,
                    'market': mma_odds.market,
                    'pulled_time': str(mma_odds.pulled_time),
                    'odds': mma_odds.odds,
                   
                    'highest_bettable_odds': mma_odds.highest_bettable_odds,
                    'sportsbooks_used': mma_odds.sportsbooks_used,
                    'market_key': mma_odds.market_key,
                    'game_date': mma_odds.game_date,
                    'game_id': mma_odds.game_id,
                    'my_game_id': my_game_id,
                    'my_event_id': my_event_id,
                    'other_id': other_row.id,
                    'other_market': other_row.market,
                    'other_pulled_time': str(other_row.pulled_time),
                    'other_odds': other_row.odds,
                   
                    'other_highest_bettable_odds': other_row.highest_bettable_odds,
                    'other_sportsbooks_used': other_row.sportsbooks_used,
                    'other_market_key': other_row.market_key,
                    'other_game_date': other_row.game_date
                }

                data.append(row_data)

        # Aliased table for subquery
        latest_odds2 = aliased(MMAOdds)

        # Subquery to get the most recent pulled_time for each game_id and market, after filtering by date
        totals_subquery = (
            session.query(
                latest_odds2.id,
                latest_odds2.game_id,
                latest_odds2.market,
                latest_odds2.market_key,
                func.row_number().over(
                    partition_by=[latest_odds2.game_id, latest_odds2.market_key, latest_odds2.market],
                    order_by=latest_odds2.pulled_time.desc()
                ).label('rank')
            )
            .filter(latest_odds2.game_date >= one_day_ago)
            .filter(latest_odds2.market_key.in_(['Main Total']))

            .subquery()
          )

        stmt2 = (
            session.query(
                MMAOdds,
                MMAGames.my_game_id,
                MMAEvents.my_event_id,
            )
            .join(totals_subquery, MMAOdds.id == totals_subquery.c.id)
            .join(MMAGames, MMAOdds.game_id == MMAGames.id)
            .join(MMAEvents, MMAOdds.event_id == MMAEvents.id)
            .filter(totals_subquery.c.rank == 1)  # Only get the most recent for each game_id and market
            .filter(MMAOdds.pulled_time >= one_day_ago)
          )

        result2 = session.execute(stmt2)
        rows2 = result2.fetchall()
        for mma_odds, my_game_id, my_event_id, in rows2:
            row_data = {
                'id': mma_odds.id,
                'market': mma_odds.market,
                'pulled_time': str(mma_odds.pulled_time),
                'odds': mma_odds.odds,
                
                'highest_bettable_odds': mma_odds.highest_bettable_odds,
                'sportsbooks_used': mma_odds.sportsbooks_used,
                'market_key': mma_odds.market_key,
                'game_date': mma_odds.game_date,
                'game_id': mma_odds.game_id,
                'my_game_id': my_game_id,
                'my_event_id': my_event_id,
                'average_market_odds': mma_odds.average_market_odds,
                'market_type': mma_odds.market_type,
                'dropdown': mma_odds.dropdown,
            }

            data.append(row_data)

        return data

      finally:
        session.close()

    # 
    def categorize_markets(self, df):
            wins_in_round_pattern = re.compile(r'wins in round \d+$')
            def categorize(row):
                market = row['market'].lower()
                
                try:
                    current_market_keys = row['market_key'].lower()
                except:
                    current_market_keys = ['']

                if "distance" in market:
                    return 'Distance (Y/N)'
                

                ###handle the round prop others

                if ('doesn' in market and 'win in round' in market) or ('ends in round' in market) or ('starts round' in market) ('start round' in market):
                    return 'Other props'
                
                ###handle all of the weird FL outliers


                if 'unanimous' in market or 'split' in market or ('tko' in market and 'doesn' in market) or ('submission' in market and 'doesn' in market) or ('decision' in market and 'doesn' in market):
                    return 'Other props'
                
                

                if 'significant' in market:
                    return 'Other props'
                
                if ' or ' in market and ('decision' in market or 'submission' in market):
                    return 'Double Chance'
                
                if ('over' in market or 'under' in market) and 'main' not in current_market_keys:
                    return 'Alt Totals'
                

                # Method of Victory (checking this first)
                mov_keywords = ['ko', 'tko', 'dq', 'submission', 'decision']

                if any(x in market for x in mov_keywords):
                    if re.search(r'\b[1-5]\b', market):
                        if '-' in market:
                            return 'Method + Round'

                mov_pattern = re.compile(r'\b(' + '|'.join(mov_keywords) + r')(,|\b|/)')
                if mov_pattern.search(market) and \
                current_market_keys != 'h2h' and \
                'scorecards' not in market:
                    return 'Method of Victory'

                
                if any(x in market for x in ['ends in round', 'wins in round', 'wins inside distance', 'starts round',"doesn't start round", "doesn't win in round" ]):
                    return 'Round props'
                

                if any(x in market for x in ['wins', 'draw', 'goes the distance', 'fight ends inside']) or market in ['over', 'under'] or any(x in current_market_keys for x in ['h2h', 'main_totals']) or any(x in market for x in ['scorecards = no action', 'ends in a draw', 'round']) or market.endswith('.5'):
                    return 'Fight lines'
                

                
                return 'Other props'


            df['market_type'] = df.apply(categorize, axis=1)
            return df
    
    def categorize_dropdown(self, df):
            
            def categorize(row):
                market = row['market'].lower()

                if 'wins by' and 'round' in market:
                    return 1
                elif ('over' or 'under') in market and 'rounds' in market:
                    return 1
                return 0

            df['dropdown'] = df.apply(categorize, axis=1)
            return df
    

    def mark_main_totals(self, df):

        desired_columns = [
            'DraftKings', 'BetMGM', 'Caesars', 'BetRivers', 'FanDuel', 'Bet365', 'Unibet', 'PointsBet', 
            'BetOnline', 'BetAnySports', 'BetUS', 'Cloudbet', 'Jazz', 'MyBookie', 'Pinnacle', 'SXBet', 
            'Bovada', 'Betway'
        ]

        # Find which of the desired columns are actually present in the DataFrame
        existing_columns = [col for col in desired_columns if col in df.columns]

        # Compute the average only for the existing columns
        df['avg_mkt_odds'] = df[existing_columns].mean(axis=1)


        def is_over_under_rounds(market):
            return market.str.contains(r'(Over|Under)', regex=True)

        filtered_df = df[is_over_under_rounds(df['market'])].copy()

        # Step 2: Extract round value
        filtered_df['round_value'] = filtered_df['market'].str.extract(r'(\d+\.?\d*)')[0].astype(float)

        # Step 3: Group by game_id and round_value, then sum the avg_mkt_odds
        grouped = filtered_df.groupby(['game_id', 'round_value'])['avg_mkt_odds'].sum().reset_index()

        # Step 4: Find the round value with the lowest sum for each game
        lowest_rounds = grouped.loc[grouped.groupby('game_id')['avg_mkt_odds'].idxmin()]

        # Step 5: Merge this information back to the filtered DataFrame
        filtered_df = filtered_df.merge(lowest_rounds[['game_id', 'round_value']], 
                                        on=['game_id', 'round_value'], 
                                        how='left', 
                                        indicator=True)

        # Step 6: Mark the rows
        filtered_df['mark'] = np.where(filtered_df['_merge'] == 'both', 'Lowest Group', 'Other')
        filtered_df = filtered_df.drop('_merge', axis=1)

        # Step 7: Apply the marks to the original DataFrame
        df = df.merge(filtered_df[['game_id', 'market', 'mark']], 
                    on=['game_id', 'market'], 
                    how='left')

        # Fill NaN values in 'mark' column for rows that were not in the filtered DataFrame
        df['mark'] = df['mark'].fillna('Not Applicable')
        df = df.replace('Main Total Under', '')
        df = df.replace('Main Total Over', '')
        df['market_key'] = np.where(df['mark'] == 'Lowest Group', 'Main Total', df['market_key'])
        columns_to_drop = ['mark', 'mark_y', 'mark_x', 'avg_mkt_odds']
        for col in columns_to_drop:
            if col in df.columns:  # Check if the column exists before attempting to drop it
                df = df.drop(columns=col)
        return df


    def format_odds(self):
        files = os.listdir(f"{os.getcwd()}/mma_raw_odds")

        file_prefix = f"{os.getcwd()}/mma_raw_odds/"

        pulled_id = uuid.uuid4()

        total_df = pd.DataFrame()



        # this is for MLB and it splits into pitching and batting data, instead we're not gonna aplit it up but instead concatenate all of them together because overlapping column names can be handled now 
        for file in files:
            if file.endswith('.csv'):
                if 'Future' not in file:
                    print(f'Formatting {file}...')
                    try:
                        file_name = (file_prefix+file).strip()
                        this_df = pd.read_csv(file_name)
                        try:
                            os.remove(file_name)
                            print(f'Deleted {file_name}')
                        except OSError as e:
                            print(f"Error deleting {file_name}: {e}")

                        #format fight_name 
                        this_df['fight_name'] = this_df['fight_name'].apply(self.process_fight_name)

                        #add pull identifier for future merge use
                        this_df['pulled_id'] = pulled_id
                        this_df.to_csv('test_here.csv')


                        this_df['matchup'] = ""
                        for col in this_df.columns:
                            if this_df[col].dtype == 'object':  # Check if column contains string data
                                this_df[col] = this_df[col].apply(lambda x: self.convert_fraction_to_float(x) if isinstance(x, str) else x)


                        this_df.replace({'▼': '', '▲': ''}, regex=True, inplace=True)

                        
        

                        current_away_fighter = ''
                        current_home_fighter = ''
                        player_count = 0
                        for idx, row in this_df.iterrows():
                            if pd.isna(row['class_name']) or row['class_name'].strip() == '':
                                if player_count == 0:
                                    player_count = 1
                                    current_away_fighter = row['market']
                                elif player_count == 1:   
                                    player_count = 2
                                    current_home_fighter = row['market']

                                    if str(current_home_fighter) < str(current_away_fighter):
                                        team_1 = str(current_home_fighter)
                                        team_2 = str(current_away_fighter)
                                    else:
                                        team_1 = str(current_away_fighter)
                                        team_2 = str(current_home_fighter)
                                    this_df.at[idx-1, 'matchup'] = team_1 + 'v.' + team_2
                                    this_df.at[idx, 'matchup'] = team_1 + 'v.' + team_2
                        
                                    this_df.at[idx-1, 'home_team'] = team_1
                                    this_df.at[idx, 'home_team'] = team_1
        
                                    this_df.at[idx-1, 'away_team'] = team_2
                                    this_df.at[idx, 'away_team'] = team_2
                                    my_game_id = team_1.replace(' ', '_') + '_' + team_2.replace(' ', '_') + '_' + row['game_date'].replace(' ', '_').split('_')[0]
                                    
                                    game_id, event_id = self.get_or_create_ids(my_game_id, row['fight_name'])


                                    this_df.at[idx - 1, 'game_id'] = int(game_id)
                                    this_df.at[idx, 'game_id'] = int(game_id)
                                    

                                    this_df.at[idx - 1, 'event_id'] = int(event_id)
                                    this_df.at[idx, 'event_id'] = int(event_id)

                                    
                                    player_count = 0
                            else:
                                    if current_home_fighter < current_away_fighter:
                                        team_1 = current_home_fighter
                                        team_2 = current_away_fighter
                                    else:
                                        team_1 = current_away_fighter
                                        team_2 = current_home_fighter
                                    this_df.at[idx, 'matchup'] = team_1 + 'v.' + team_2
                                    this_df.at[idx, 'away_team'] = team_2
                                    this_df.at[idx, 'home_team'] = team_1
                                    my_game_id = team_1.replace(' ', '_') + '_' + team_2.replace(' ', '_') + '_' + row['game_date'].replace(' ', '_').split('_')[0]
                                    game_id, event_id = self.get_or_create_ids(my_game_id, row['fight_name'])
                                    this_df.at[idx, 'game_id'] = int(game_id)

                                    this_df.at[idx, 'event_id'] = int(event_id)

                    except Exception as e:
                        print(f"Error processing {file_name}: {e}")
                            # Delete the problematic file
                        try:
                                os.remove(file_name)
                                print(f"Deleted {file_name} due to error")
                                continue
                        except OSError as delete_error:
                                print(f"Error deleting {file_name}: {delete_error}")
                                continue

                    total_df = pd.concat([total_df, this_df])

        total_df.to_csv('total_df.csv', index=False)

        bestFightOdds = None

        try:
            scraper = BestFightOddsScraper('https://www.bestfightodds.com/')
            events = scraper.scrape_event_data(i)
            bestFightOdds = scraper.format_odds()  # This may raise an error
            bestFightOdds.to_csv('bestFightOdds.csv', index=False)
        except Exception as e:
            print(f"Error scraping best fight odds: {e}")

        # Check if bestFightOdds was successfully created
        if (
                bestFightOdds is not None 
                and not bestFightOdds.empty 
                and 'market' in total_df.columns 
                and 'game_id' in total_df.columns 
                and 'market' in bestFightOdds.columns 
                and 'game_id' in bestFightOdds.columns
            ):
            merged_df = total_df.merge(bestFightOdds, on=['market', 'game_id'], how='left', suffixes=('', '_bestFightOdds'))
        else:
            print("bestFightOdds is empty or was not created. Skipping merge operation.")
            merged_df = total_df.copy()

        # Drop columns from bestFightOdds that conflict with total_df
        # This will remove columns like 'team_bestFightOdds', keeping only the ones from total_df
        for col in merged_df.columns:
            if col.endswith('_bestFightOdds'):
                # Find the base column without the suffix
                base_col = col.replace('_bestFightOdds', '')
                
                # Check if the base column exists in the merged dataframe
                if base_col in merged_df.columns:
                    # Fill NaNs in the base column with values from the bestFightOdds column
                    merged_df[base_col].fillna(merged_df[col], inplace=True)
                
                # After filling, drop the _bestFightOdds column
                merged_df.drop(col, axis=1, inplace=True)

                # Save or return the final merged DataFrame
                merged_df.to_csv('merged_output.csv', index=False)



        ###Convert odds
        exclude_columns = ['class_name', 'matchup', 'home_team', 'away_team', 'market', 'game_date', 'game_id', 'fight_name', 'event_id', 'pulled_id']
        for col in merged_df.columns:
            if col not in exclude_columns:
                merged_df[col] = merged_df[col].apply(self.american_to_decimal)

        ##Hightest bettable odds 
        odds_cols = [col for col in merged_df.columns if col not in exclude_columns]

        #Drop empty bad rows 
        merged_df[odds_cols] = merged_df[odds_cols].replace('', np.nan)

        # Drop rows where all values in odds_cols are NaN
        merged_df = merged_df.dropna(subset=odds_cols, how='all')

    
        # Calculate the maximum odds for each row   
        merged_df['highest_bettable_odds'] = merged_df[odds_cols].max(axis=1)

        exclude_columns.append('highest_bettable_odds')

        # Calculate the average bettable odds, ignoring NaN values
        merged_df['average_bettable_odds'] = merged_df[odds_cols].mean(axis=1, skipna=True)

        # Exclude the new column from further processing if needed
        exclude_columns.append('average_bettable_odds')


        merged_df['sportsbooks_used'] = merged_df.apply(lambda row: self.find_matching_columns(row, odds_cols), axis=1)
        
        exclude_columns.append('sportsbooks_used')

        first_total_flag = [False, False]

        merged_df['market_key'] = merged_df.apply(lambda row: self.market_key_map(row, first_total_flag), axis=1)


        merged_df = self.get_favored_team(merged_df)
        merged_df.to_csv('aver_favorite.csv', index=False)
        # Step 1: Identify game_ids where the 'market' is null or an empty string
        invalid_game_ids = merged_df.loc[merged_df['market'].isnull() | (merged_df['market'] == ''), 'game_id'].unique()

# Step 2: Remove rows where the 'game_id' is in the list of invalid game_ids
        merged_df = merged_df[~merged_df['game_id'].isin(invalid_game_ids)]


        exclude_columns.append('market_key')
        # Generate the `my_game_id` for the current row

        merged_df = merged_df.replace({np.nan: None})
        merged_df['odds'] = merged_df.apply(lambda row: json.dumps({col: row[col] for col in odds_cols}), axis=1)
        # this_df.to_csv('after_all_collection.csv', index = False)

        merged_df.to_csv('before_categorize.csv', index=False)
        merged_df = self.categorize_markets(merged_df)
        merged_df.to_csv('after_categorize.csv', index=False)
        merged_df = self.mark_main_totals(merged_df)
        merged_df = self.categorize_dropdown(merged_df)


        merged_df.to_csv('final_output.csv', index=False)






        
        exclude_columns.append('game_id')


        with self.db_manager.get_engine().begin() as conn:
            for _, row in merged_df.iterrows():
                # Create a dictionary for each row
                row_dict = {
                    'market': row['market'],
                    'odds': row['odds'],
                    'class_name': row['class_name'],
                    'matchup': row['matchup'],
                    'highest_bettable_odds': row['highest_bettable_odds'],
                    'sportsbooks_used': str(row['sportsbooks_used']),
                    'market_key': row['market_key'],
                    'game_date': row['game_date'],
                    'game_id': row['game_id'],
                    'event_id' : row['event_id'],
                    'pulled_time': datetime.now(),
                    'average_market_odds' : row['average_bettable_odds'],
                    'market_type':row['market_type'],
                    'dropdown': int(row['dropdown']),
                    'pulled_id': row['pulled_id'],
                    'favored_team': row['favored_team'],
                    'underdog_team': row['underdog_team'],
                }

                # Insert the row into the mma_odds table
                stmt = insert(self.mma_odds).values(**row_dict)
                conn.execute(stmt)






             

    def add_priority_column(self, merged_df):
        # Function to extract priority number from 'market' and add a 'priority' column
        merged_df['priority'] = merged_df['market'].apply(
            lambda x: int(re.search(r'\d+', x).group()) if re.search(r'\d+', x) else None
        )
        return merged_df

    def get_favored_team(self, this_df):
        # Filter for h2h rows
        h2h_df = this_df[this_df['market_key'] == 'h2h']

        # Dictionary to store favored and underdog teams by game_id
        team_mapping = {}

        # List to hold the reordered h2h rows
        reordered_groups = []

        # Group by game_id and process each group
        for game_id, group in h2h_df.groupby('game_id'):
            # Ensure there are exactly two rows for comparison
            if len(group) == 2:
                # Sort by highest_bettable_odds to ensure the favored team comes first
                group_sorted = group.sort_values(by='highest_bettable_odds', ascending=False)
                
                # Get the favored and underdog team based on the sorted order
                favored_team = group_sorted.iloc[1]['market']
                underdog_team = group_sorted.iloc[0]['market']
                
                # Store the favored and underdog teams in a dictionary for this game_id
                team_mapping[game_id] = {
                    'favored_team': favored_team,
                    'underdog_team': underdog_team
                }

                # Add the reordered group to the list
                reordered_groups.append(group_sorted)
            else:
                # Handle cases where there are not exactly 2 rows
                team_mapping[game_id] = {
                    'favored_team': None,
                    'underdog_team': None
                }

        # Concatenate all reordered h2h groups into one DataFrame
        reordered_h2h_df = pd.concat(reordered_groups)

        # Now we apply the favored and underdog teams to the h2h_df using the mapping
        reordered_h2h_df['favored_team'] = reordered_h2h_df['game_id'].map(lambda x: team_mapping[x]['favored_team'])
        reordered_h2h_df['underdog_team'] = reordered_h2h_df['game_id'].map(lambda x: team_mapping[x]['underdog_team'])

        # Merge the favored/underdog team mapping back into the original dataframe
        fav_underdog_df = pd.DataFrame(team_mapping).T.reset_index().rename(columns={'index': 'game_id'})

        # Merge favored/underdog teams into the original dataframe (for non-h2h rows too)
        this_df = this_df.merge(fav_underdog_df, on='game_id', how='left')

        # Replace the h2h rows in the original dataframe with the reordered ones
        non_h2h_df = this_df[this_df['market_key'] != 'h2h']
        this_df = pd.concat([non_h2h_df, reordered_h2h_df]).sort_index()

        return this_df

    def find_fight_name_and_date(self, soup):
        outer_divs = soup.find_all("div", class_="MuiPaper-root")
        target_div = None
        for div in outer_divs:
            if div.find("table"):
                target_div = div
                break

        # Step 3: If the target div is found, find the previous sibling <a> element
        if target_div:
            a_sibling = None
            current_sibling = target_div.find_previous_sibling()
            
            while current_sibling:
                if current_sibling.name == "a":
                    a_sibling = current_sibling
                    break
                current_sibling = current_sibling.find_previous_sibling()
            
            # Step 4: Within the <a> sibling, find the <div> and then the <p> tag
            if a_sibling:
                # Find the <div> within the <a> sibling
                inner_div = a_sibling.find("div")
                
                if inner_div:
                    # Find the <p> tag within the <div>
                    p_tag = inner_div.find("p")
                    
                    if p_tag:
                        # Extract and print the text from the <p> tag
                        text = p_tag.get_text(strip=True)
                        print(f"Text found in <p>: {text}")
                        
                        span_tag = inner_div.find("span")
                        if span_tag: 
                            date_text = span_tag.get_text(strip=True)
                            return (text, date_text)
                        else:
                            print("No span found")
                            return None
                    
                    else:
                        print("No <p> tag found within the <div> inside the <a> sibling.")
                        return None
                else:
                    print("No <div> found within the <a> sibling.")
                    return None
            else:
                print("No previous <a> element found.")
                return None
        else:
            print("No <div> containing the table found.")
            return None

    def process_fight_name(self,name):
        pattern = r'(?:January|February|March|April|May|June|July|August|September|October|November|December) \d{1,2}$'

        parts = name.split(':')
    
        if len(parts) > 2:
            # Return the first two parts if there are three or more segments
            return ':'.join(parts[:2]).strip()
        elif len(parts) == 2:
            # Return the first part if there are two segments
            return parts[0].strip()
        else:
            # Remove date information if there are no colons
            return re.sub(pattern, '', name).strip()
        
    

    def replace_fraction(self,symbol):
        """Replace fraction symbol ½ with its decimal equivalent."""
        return symbol.replace("½", ".5")

    def convert_fraction_to_float(self,text):
        """Convert fraction symbol ½ to float while keeping other text intact."""
        # Replace fraction symbol with decimal equivalent
        text_decimal = self.replace_fraction(text)
        # Extract all numbers with decimals
        result = re.sub(r'(\d+)\.(5)', r'\1.5', text_decimal)
        return result
    
    def get_or_create_ids(self, my_game_id, my_event_id):
        session = self.db_manager.create_session()
        
        try:
            # Step 1: Check if the my_game_id exists in mma_games
            stmt = select(self.mma_games.c.id, self.mma_games.c.event_id).where(self.mma_games.c.my_game_id == my_game_id)
            result = session.execute(stmt).fetchone()

            if result:
                # Case 2: my_game_id exists, return the existing game id and event id
                game_id, event_id = result
                return game_id, event_id

            
            # Case 1: my_game_id does not exist, check if my_event_id exists in mma_events
            stmt = select(self.mma_events.c.id).where(self.mma_events.c.my_event_id == my_event_id)
            event_id = session.execute(stmt).scalar()

            if not event_id:
                # If my_event_id does not exist, create a new entry in mma_events
                stmt = insert(self.mma_events).values(my_event_id=my_event_id)
                event_result = session.execute(stmt)
                session.commit()
                event_id = event_result.inserted_primary_key[0]  # Get the new event id
            
            # Step 3: Create a new entry in mma_games with the fetched/created event_id
            stmt = insert(self.mma_games).values(my_game_id=my_game_id, event_id=event_id)
            game_result = session.execute(stmt)
            session.commit()
            game_id = game_result.inserted_primary_key[0]  # Get the new game id

            # Step 4: Return the new ids for both mma_games and mma_events
            return game_id, event_id
        
        finally:
            session.close()    
    def market_key_map(self, row, first_totals_over_under):
        if pd.isna(row['class_name']) or row['class_name'].strip() == '':
            first_totals_over_under[0] = False
            first_totals_over_under[1] = False
            return 'h2h'
        
        if 'Over' in row['market']:
            if first_totals_over_under[0] == False and row['highest_bettable_odds'] >= 1.4 and row['highest_bettable_odds'] <= 3.5:
                first_totals_over_under[0] = True
                return 'Main Total Over'
            else:
                return 'totals'
                
        elif 'Under' in row['market']:
            if first_totals_over_under[1] == False and row['highest_bettable_odds'] >= 1.4 and row['highest_bettable_odds'] <= 3.5:
                first_totals_over_under[1] = True
                return 'Main Total Under'
            else: 
                return 'totals'
        else:
            return ''

    def find_matching_columns(self, row, bettable_books):
        matching_cols = [col.title() for col in bettable_books if row[col] == row['highest_bettable_odds']]
        return list(set(matching_cols))


    def american_to_decimal(self, american_odds):
        if pd.isna(american_odds):
            return np.nan
        
        if isinstance(american_odds, float):
            american_odds_int = int(american_odds)
        elif isinstance(american_odds, int):
            american_odds_int = american_odds

        elif isinstance(american_odds, str):
            if american_odds.startswith('+'):
                american_odds = american_odds[1:]

            american_odds_int = int(american_odds)

        if american_odds_int >= 0:
            decimal = (american_odds_int / 100) + 1
        else:
            decimal = (100 / abs(american_odds_int)) + 1

        return decimal


scraper = BestFightOddsScraper('https://www.bestfightodds.com/')
fightOddsIO = fightOddsIOScraper('https://fightodds.io/')
i = 0
while True:
    i += 1
    try:
        fightOddsIO.scrape_event_data(i)
    except Exception as e:
        print(f"Error occurred while scraping event {i}: {e}")
        continue
    try:
        fightOddsIO.format_odds()
    except Exception as e:
        print(f"Error occurred while formatting odds for event {i}: {e}")
        continue
    try:
        fightOddsIO.get_mma_data_for_cache()
    except Exception as e:
        print(f"Error occurred while getting MMA data for event {i}: {e}")
        continue
    try:
        fightOddsIO.get_mma_game_data_for_cache()
    except Exception as e:
        print(f"Error occurred while getting MMA game data for event {i}: {e}")
        continue
    
    print("Events Done!")
    time.sleep(300)