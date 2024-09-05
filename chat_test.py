from functionality.db_manager import DBManager
import pandas as pd 
from openai import OpenAI
import os
from functionality.models import ChatQuestions
import redis
from pandasql import sqldf
from io import StringIO
import ast
from flask import Flask, request, jsonify
import openai



class Chat_test():
    def __init__(self, redis_client):
        # self.DB = DBManager()
        self.redis_client = redis_client

        self.DB = DBManager()

        self.client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

    def create_table_definition_prompt(self):
        prompt =  """
    ### SQL Query Generation Prompt
    #### Objective: Write an SQL query to retrieve specific information from the database based on user requirements. Use the provided properties and tables for reference.
    #### Properties:
        - `props_data` Table:
            - Columns: active, date, points_scored, plus_minus, team, location, opponent, outcome, seconds_played, made_field_goals, attempted_field_goals, made_three_point_field_goals, attempted_three_point_field_goals, made_free_throws, attempted_free_throws, offensive_rebounds, defensive_rebounds, assists, steals, blocks, turnovers, personal_fouls, game_score, player
        - `nba_extra_info` Table:
            - Columns: date, Start (ET), away_team, PTS, home_team, PTS.1, Arena, winning_team, home_away_neutral, losing_team, team_1, team_2, home_team_conference, home_team_division, away_team_conference, away_team_division, team_1_division, team_2_division, team_1_conference, team_2_conference, day_of_week, time, compare_time, day_night, commence_date, my_game_id
    #### Instructions:
    0. Write the SQL command for MySQL
    1. Clearly state the goal or question you want the SQL query to address.
    2. Break down the task into logical steps.
    3. Explicitly mention any conditions or filters that should be applied.
    4. Specify the output format or additional calculations if needed.
    5. If aggregating data, use proper aggregate functions and include a GROUP BY clause.
    6. Use semicolons to separate multiple queries if necessary.
    7. Include the initial `SELECT` statement in your query.


    """
        
        prompt =  """
    ### SQL Query Generation Prompt
    #### Objective: Write an SQL query to retrieve specific information from the database based on user requirements. Use the provided properties and tables for reference.
    #### Properties (I have provided some descriptions for columns that aren't self explanitory):
        - `df` Table:
            - Columns: sport_title, game_id (arbitrary id for each sport game), home_team,	away_team, market (market of the bet), wager (the full wager title), wagers_other (the opposite side of the wager), value (value of the spread, EX. -19.5), highest_bettable_odds (the odds of the bet), highest_bettable_odds_other_X (odds of the other side of the bet), ev (a value for positive ev), snapshot_time (time bet was pulled), sportsbooks_used, game_date,	sport_title_display, sport_league_display, market_display, wager_display, wager_display_other
            - Example row for team bet:  basketball_nba, 2e87431523192387ff81ff8d741a0be0, Miami Heat, Golden State Warriors, alternate_spreads, Miami Heat_-19.5, Golden State Warriors_19.5, -19.5, 12.645, 15, 1.02, 1.02, 0.074643249, 11.96487377, 40:29.8, ['Fanduel'], 2024-03-26T23:40:00Z, Pro Basketball, NBA	Spread, Miami Heat -19.5, Golden State Warriors +19.5	
            - Example row for player bet: basketball_nba, 2e87431523192387ff81ff8d741a0be0, Miami Heat, Golden State Warriors, player_points_rebounds_assists, Chris Paul_Over_18.5, Chris Paul_Under_18.5, Over, 1.886666667, 2.03, 1.87, 1.835555556, 0.493134328, 0.106268657, 40:38.6, ['Pinnacle'], 2024-03-26T23:40:00Z, Pro Basketball, NBA	Player Points + Rebounds + Assists, Chris Paul Over 18.5, Chris Paul Under 18.5
            - Ensure when searching for sportsbooks used you use the like command instead of the = command because of the nature of the string. Also keep in mind that for sportsbooks only the first letter is capitalized
            - When looking for a team, make sure the search matches the structure (Ex. Los Angeles Lakers), any abbreviations of city or team should be interpreted in that format
            - When looking for a team, look in the home_team and the away_team columns
            - If the search seems like it contains a name, look in the wager or wagers_other columns using the like command
    #### Instructions:
    0. Write the SQL command for me to pull from the df table
    1. Make sure to retrieve these key columns along with any different ones you need: home_team, away_team, wager, highest_bettable_odds, sportsbooks_used, ev
    2. If aggregating data, use proper aggregate functions and include a GROUP BY clause.
    3. Include the initial `SELECT` statement in your query.


    """
        return prompt

    def create_another_table_definition_prompt(self):
        # prompt2 = '''### And another sql table with additional information about full games. you can merge these two tables on the 'my_game_id' column. with its properties:
        # #
        # # nba_extra_info(date,Start (ET), away_team, PTS, home_team, PTS.1, Arena, winning_team, home_away_neutral, losing_team,team_1, team_2, home_team_conference, home_team_division, away_team_conference, away_team_division, team_1_division, team_2_division, team_1_conference, team_2_conference, day_of_week, time,compare_time, day_night, commence_date, my_game_id)
        # #
        # '''
        prompt2 = ''
        return prompt2


    def combine_prompts(self, query_prompt):
        definition = self.create_table_definition_prompt()
        second_def = self.create_another_table_definition_prompt()
        query_init_string = f'### A query using one or more of the tables provided to answer: {query_prompt}\nSELECT'
        return definition + query_init_string
    
    def handle_response(self, query):
        if query.startswith(' '):
            query = 'SELECT' + query
        return query
    
    def generate_combined_response(self, user_prompt, response):
            new_query = f"""Create a concise answer using the prompt and the response.
            prompt: {user_prompt}
            response: {response}
            """

            print(new_query)

            # Use GPT-3 to generate the combined response
            completion = self.client.completions.create(
                model="gpt-3.5-turbo-instruct",
                prompt=new_query,
                temperature=0,
                max_tokens=150,
                top_p=1.0,
                frequency_penalty=0.0,
                presence_penalty=0.0,
            )
            
            return completion.choices[0].text.strip()

    def ask(self, prompt):

        nlp_text = prompt
        print(nlp_text)

        prompt = self.combine_prompts(nlp_text)

        response = self.client.completions.create(
            model="gpt-3.5-turbo-instruct",
            prompt=prompt,
            temperature=0,
            max_tokens=150,
            top_p=1.0,
            frequency_penalty=0.0,
            presence_penalty=0.0,
            stop=["#", ";"]
        )

        #Access the generated text
        generated_text = response.choices[0].text

        print(generated_text)

        generated_text = self.handle_response(generated_text)

        df = self.read_cached_df('pos_ev_dash_cache')
        df = df.applymap(str)

        new_df = sqldf(generated_text, locals())

        recommendations = []
        i = 0
        for index,row in new_df.iterrows():
            if i > 4:
                break
            # recommendation = (f"{index + 1}. In the {row['home_team']} vs. {row['away_team']} game a potential bet is {row['wager']} with odds: {row['highest_bettable_odds']} on {ast.literal_eval(row['sportsbooks_used'])[0]} and with +EV: {round(float(row['ev']), 2)}%")
            recommendation = f"{index + 1}. In the {row['home_team']} vs. {row['away_team']} game a potential bet is {row['wager']} with odds: {row['highest_bettable_odds']} on {ast.literal_eval(row['sportsbooks_used'])[0]} and with +EV: {round(float(row['ev']), 2)}%"
            recommendations.append(recommendation)
            i += 1
        if len(recommendations) > 0:
            response_text = "\n\n".join(recommendations)

        else:
            # prompt = '''#### Create a response that details why this query would not work on the provided dataset that contains these specifications:
            #             #### Properties:
            #             - `props_data` Table:
            #             - Columns: active, date, points_scored, plus_minus, team, location, opponent, outcome, seconds_played, made_field_goals, attempted_field_goals, made_three_point_field_goals, attempted_three_point_field_goals, made_free_throws, attempted_free_throws, offensive_rebounds, defensive_rebounds, assists, steals, blocks, turnovers, personal_fouls, game_score, player
            #             - `nba_extra_info` Table:
            #             - Columns: date, Start (ET), away_team, PTS, home_team, PTS.1, Arena, winning_team, home_away_neutral, losing_team, team_1, team_2, home_team_conference, home_team_division, away_team_conference, away_team_division, team_1_division, team_2_division, team_1_conference, team_2_conference, day_of_week, time, compare_time, day_night, commence_date, my_game_id
            #             #### Query:
                    
            #         '''
            # prompt += generated_text
            # response = self.client.completions.create(
            # model="gpt-3.5-turbo-instruct",
            # prompt=prompt,
            # temperature=0,
            # max_tokens=150,
            # top_p=1.0,
            # frequency_penalty=0.0,
            # presence_penalty=0.0,
            # stop=["#", ";"]
            # ) 
            # response_text = response.choices[0].text
            response_text = "No recommendations found. Try rewording your request or trying a different team, player, or wager type."
        try:
            self.session = self.DB.create_session()
            new_question = ChatQuestions(question=prompt, response=generated_text, worked_bool=True, full_response=response_text)
            self.session.add(new_question)
            self.session.commit()
            self.session.close()
        except Exception as e:
            print(e)


        return response_text

        
    def read_cached_df(self, path):
        serialized_df = self.redis_client.get(path)
        if serialized_df:
            serialized_df = serialized_df.decode('utf-8')
            cached_df = pd.read_json(StringIO(serialized_df))
            return cached_df
        else:
            raise FileNotFoundError(f"No cached DataFrame found at path: {path}")