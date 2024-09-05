from functionality.db_manager import DBManager
import pandas as pd 
from openai import OpenAI
import os
from functionality.models import ChatQuestions


class Chat():
    def __init__(self):
        self.DB = DBManager()

        try:
            self.session = self.DB.create_session()
            # self.df2 = pd.read_sql_table('nba_extra_info', con=self.DB.get_engine())
        except Exception as e:
            print(e)
        finally:
            self.session.close()

        self.client = OpenAI(api_key='sk-proj-AEkdKNg0ipRal37uOnqhT3BlbkFJUHjOzdqjb5gCyYLIi0mq')

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
        return prompt

    def create_another_table_definition_prompt(self):
        prompt2 = '''### And another sql table with additional information about full games. you can merge these two tables on the 'my_game_id' column. with its properties:
        #
        # nba_extra_info(date,Start (ET), away_team, PTS, home_team, PTS.1, Arena, winning_team, home_away_neutral, losing_team,team_1, team_2, home_team_conference, home_team_division, away_team_conference, away_team_division, team_1_division, team_2_division, team_1_conference, team_2_conference, day_of_week, time,compare_time, day_night, commence_date, my_game_id)
        #
        '''
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

        print(prompt)

        print(prompt)

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

        # Access the generated text
        generated_text = response.choices[0].text

        print(generated_text)

        generated_text = self.handle_response(generated_text)

        try:
            result_df = pd.read_sql_query(generated_text, con=self.DB.get_engine())
            print("Query executed successfully. Results:")
            print(result_df)
            
            value_to_convert = result_df.iloc[0, 0]
            print("++++++++++")
            print(value_to_convert) 

            final_response = self.generate_combined_response(nlp_text, value_to_convert)
            self.session = self.DB.create_session()
            new_question = ChatQuestions(question=nlp_text, response=final_response, worked_bool=True)
            self.session.add(new_question)
            self.session.commit()
            self.session.close()

            return final_response


        except Exception as e:
            print("Error executing the query:")
            self.session = self.DB.create_session()
            final_response = self.generate_combined_response(nlp_text, 'Sorry, I could not find an answer to your question. Please try again.')
            new_question = ChatQuestions(question=nlp_text, response=str(e), worked_bool=False)
            self.session.add(new_question)
            self.session.commit()
            self.session.close()

            return final_response
        
