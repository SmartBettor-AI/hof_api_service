import pandas as pd
from .user import User
from .util import american_to_decimal
import os
import numpy as np
import math
from datetime import datetime, timedelta
import ast
import stripe
import logging
from functionality.models import RememberToken
from functionality.models import LoginInfo, MMAEvents, MMAOdds, MMAGames
from sqlalchemy.orm.exc import NoResultFound
from ast import literal_eval
import pytz
from werkzeug.security import generate_password_hash, check_password_hash
import flock as flock
import secrets
import hashlib
import redis
redis_client = redis.Redis(host='localhost', port=6379, db=0)
from sqlalchemy import desc, func, select, and_
from sqlalchemy.orm import aliased
import logging
logging.basicConfig(level=logging.INFO)
from decimal import Decimal
logger = logging.getLogger(__name__)
from pytz import timezone

import psutil
process = psutil.Process(os.getpid())
import time



# Configure the logging level for the stripe module
logging.getLogger("stripe").setLevel(logging.ERROR)

STRIPE_PUBLIC_KEY = 'pk_live_51Nm0vBHM5Jv8uc5M5hu3bxlKg6soYb2v9xSg5O7a9sXi6JQJpl7nPWiNKrNHGlXf5g8PFnN6sn0wcLOrixvxF8VH00nVoyGtCk'

STRIPE_PRIVATE_KEY = os.environ.get("STRIPE_API_KEY")
stripe.api_key = STRIPE_PRIVATE_KEY


class database():
    

    def __init__(self, db_manager):
        self = self
        self.db_manager = db_manager


    def get_all_usernames(self):
      try:
        session = self.db_manager.create_session()
        usernames = session.query(LoginInfo.username).all()

        usernames_list = [username[0] for username in usernames]

        self.users = usernames_list
      except Exception as e:
        print(e)
        return str(e)
      finally:
        session.close()
        return
      

    def get_scores(self):
      try:
          session = self.db_manager.create_session()
          read_in =  pd.read_sql_table('scores', con=self.db_manager.get_engine())
      except Exception as e:
        print(e)
        return str(e)
      finally:
        session.close()
        return read_in
    

    def add_user(self, firstname, lastname, username, password, phone, bankroll, sign_up_date, payed, how_heard, referral_name, other_source, unitSize, kelley_criterion):
       try:
          new_user = User(username)
          password = generate_password_hash(password, method='pbkdf2:sha256')

          new_user.create_user(firstname, lastname, username, password, phone, bankroll, sign_up_date, payed,how_heard, referral_name, other_source, unitSize, kelley_criterion, self.db_manager)
          self.users = self.get_all_usernames()
          logger.info(f'successfully created user for username {username}')
       except Exception as e:
          logger.info(e)
          return str(e)
       return


    def check_account(self,username):
      try:
        # Create a session
        session = self.db_manager.create_session()
        
        # Query the user's record by username
        user = session.query(LoginInfo).filter_by(username=username).first()
        
        if user:
            
            # Calculate the time difference
            time_difference = datetime.now() - (datetime.strptime(user.date_signed_up, '%Y-%m-%d %H:%M:%S.%f'))
            # Calculate the days difference
            days_difference = time_difference.days
            
            # Check if the user is paid or signed up within the last 8 days
            if user.payed or days_difference <= 8:
                return True
            else:
                return False
        else:
            return False
      except Exception as e:
        print(e)
        return str(e)
      finally:
        session.close()
     

    def check_login_credentials(self, username, password):

      
      try:
        # Create a session
        session = self.db_manager.create_session()

        # Query the user by username
        try:
            user = session.query(LoginInfo).filter_by(username=username).one()
        except NoResultFound:
            return False
# Check if the password matches     
        if check_password_hash(user.password, password):
            return True
        else:
            return False
      except Exception as e:
        logger.info(e)
        return str(e)
      finally:
        session.close()
    

    def check_duplicate_account(self,username):
      print("checking duplicate account")
      self.check_payments(username)
      try:
        # Create a session
        session = self.db_manager.create_session()

        # Query the user's record by username
        user = session.query(LoginInfo).filter_by(username=username).first()

        if user and user.payed:
            # If the user exists and is paid, you can remove the user record
            # from the database using SQLAlchemy
            session.delete(user)
            session.commit()
            return True
        else:
            return False
      except Exception as e:
        print(e)
        return str(e)
      finally:
        session.close()


    def get_permission(self, username):
      try:

          customers = stripe.Customer.list(email=username.lower())

          for customer in customers.data:

            customer_id = customer['id']

            if len(stripe.Subscription.list(customer=customer_id)) > 0:

              user_subscriptions = stripe.Subscription.list(customer=customer_id)
              if user_subscriptions['data']:
                highest_price = 0
                highest_price_subscription = None

                for subscription in user_subscriptions['data']:
                  status = subscription['status']
                  if status == 'trialing' or status == 'active':
                    for item in subscription['items']['data']:

                      price = item['price']['unit_amount']
                      price_id = item['price']['id']
                      if price > highest_price:
                          highest_price = price
                          highest_price_subscription = subscription
                          highest_price_price_id = price_id
                      if highest_price_subscription:
                          highest_price_status = highest_price_subscription.get('status')
                          if highest_price_status == 'trialing':
                            highest_price_status = 'active'
                          if highest_price_status == 'incomplete':
                                        # Handle payment failure case
                            return ({
                              'status': highest_price_status,
                              'permission': 'payment_failed'
                             })

                          return({
                            'status':highest_price_status,
                            'permission':self.get_plan_from_price_id(highest_price_price_id)
                            })
                      
          return({'status': 'none', 
                  'permission': 'free' 
                  })

      except stripe.error.StripeError as e:
            print(f"Stripe Error: {e}")
            return None


    def get_plan_from_price_id(self, price_id):
       plans = {
        'price_1OSlSoHM5Jv8uc5MR6vK5xrA':'ev',
        'price_1OZhXvHM5Jv8uc5MyZO28LI1': 'ev',
        'price_1OG9CDHM5Jv8uc5MTtdQOZMv': 'standard',
        'price_1OZhbIHM5Jv8uc5MAJ16Vs1h': 'standard',
        'price_1NqdGPHM5Jv8uc5MkYrJm2UX': 'premium',
        'price_1OZhdhHM5Jv8uc5MaLORELDu': 'premium', 
        'price_1OG9DhHM5Jv8uc5MfiE2UdHR': 'premium',
        'price_1POpxLHM5Jv8uc5MpCKm1udo': 'premium'
      }
       return plans.get(price_id, 'Not found in plans')
 
 
    def get_user_bank_roll(self, user):
      try:
        # Create a session
        session = self.db_manager.create_session()

        # Query the user's bankroll by username

        result = session.query(LoginInfo.bankroll).filter_by(username=user).order_by(desc(LoginInfo.date_signed_up)).first()


        if result is None:
          raise ValueError("User not logged in most likely")
        else:
            user_bankroll = result[0]

        if user_bankroll is not None:
            return int(user_bankroll)
        else:
          raise ValueError("Error occured getting user bankroll")
      except Exception as e:
        print(e)
        raise ValueError("Error occured getting user bankroll")
      finally:
        session.close()


    def get_user_account_info(self, user):
      try:
          session = self.db_manager.create_session()
          user_info = session.query(LoginInfo).filter_by(username=user).order_by(desc(LoginInfo.date_signed_up)).first()
          if user_info:
              return {
                  'bankroll': user_info.bankroll if user_info.bankroll is not None else None,
                  'first_name': user_info.firstname,
                  'last_name': user_info.lastname,
                  'email': user_info.username,
                  'phone_number': user_info.phone,
                  'kelley_criterion': user_info.kelley_criterion,
                  'date_signed_up': user_info.date_signed_up,
                  'unitSize': user_info.unitSize,
              }
          else:
              return None

      except Exception as e:
          print(e)
          return str(e)
      finally:
          session.close()


    def update_bankroll(self, username, amount):
        try:
            session = self.db_manager.create_session()
            
            # Update all entries for this username
            result = session.query(LoginInfo).filter_by(username=username).update({'bankroll': amount})
            
            session.commit()
            return True if result > 0 else False
        except Exception as e:
            logger.error(f"Error updating bankroll: {str(e)}")
            session.rollback()
            return False
        finally:
            session.close()

    def update_unitSize(self, username, amount):
        try:
            return_dict = {}
            session = self.db_manager.create_session()
            user = session.query(LoginInfo).filter_by(username=username).first()


            if user:
              kelley_criterion = Decimal(str(user.kelley_criterion))
            # Update both kelley_criterion and bankroll
              result = session.query(LoginInfo).filter_by(username=username).update({
                  'unitSize': Decimal(str(amount)),
                  # 'bankroll': round(Decimal(str(amount)) / kelley_criterion, 2)
              })
            
            
            session.commit()
            return_bool = True if result > 0 else False
            return_dict['return_bool'] = return_bool
            return_dict['return_kelley'] = kelley_criterion
            return return_dict
        except Exception as e:
            logger.error(f"Error updating unit size: {str(e)}")
            session.rollback()
            return_dict['return_bool'] = False
            return_dict['return_kelley'] = 0
            return return_dict
        finally:
            session.close()

    def update_kelleyCriterion(self, username, amount):
        
        try:
            return_dict = {}
            session = self.db_manager.create_session()
            
            # Update all entries for this username

            user = session.query(LoginInfo).filter_by(username=username).first()


            if user:
              unitSize = Decimal(str(user.unitSize))

            # Update both kelley_criterion and bankroll
              result = session.query(LoginInfo).filter_by(username=username).update({
                  'kelley_criterion': Decimal(str(amount)),
                  # 'bankroll': round(unitSize / Decimal(str(amount)), 2)
              })

            # bankroll = unitSize / kelley_criterion
            session.commit()
            return_bool = True if result > 0 else False
            return_dict['return_bool'] = return_bool
            return_dict['return_unitSize'] = unitSize
            return return_dict
        except Exception as e:
            logger.error(f"Error updating kelley criterion: {str(e)}")
            session.rollback()
            return_dict['return_bool'] = False
            return_dict['return_unitSize'] = 0
            return return_dict
        finally:
            session.close()


    def get_recommended_bet_size(self, bankroll, df):
       
       df['decimal_highest_bettable_odds'] = df['highest_bettable_odds'].apply(american_to_decimal)

       df['win_prob'] =  (1 / df['average_market_odds']) 

       bankroll = float(bankroll)

       df['bet_amount'] = ((df['win_prob'] * df['decimal_highest_bettable_odds']) - 1) / (df['decimal_highest_bettable_odds']-1) * 0.5 * bankroll

       df['bet_amount'] = df['bet_amount'].round(2)

       return df
   

    def get_live_bet_dash_data(self, bankroll):
       
       def minutes_seconds(row):
          
          seconds = int(float(row['time_difference_seconds']))

          if seconds < 60:
            row['time_difference_formatted'] = f'{seconds} sec'

          elif seconds >= 60 and seconds < 3600:
            minutes = math.floor(seconds / 60)
            new_seconds = (seconds % 60)
            row['time_difference_formatted'] = f'{minutes} min {new_seconds} sec'
             
          else:
            hours = math.floor(seconds / 3600)
            seconds_after_hour = seconds % 3600
            new_minutes = math.floor(seconds_after_hour / 60)
            new_seconds = seconds_after_hour % 60
            row['time_difference_formatted'] = f'{hours} hours {new_minutes} min {new_seconds} sec'
          
          return row

       def format_list_of_strings(strings):
           return ', '.join(strings[0])
      
       def calculate_accepted_bettable_odds(row):
        value_new = row['highest_bettable_odds']
        if value_new < 0:
          if value_new < -500:
             value_new = value_new + (value_new * 0.1)
          else:
             value_new = value_new + (value_new * 0.05)
        else:
          if value_new > 500:
             value_new = value_new - (value_new * 0.1)
          else:
             if (value_new - (value_new * 0.05)) <= 100: 
                less_than_100 = 100 - (value_new - (value_new * 0.05))
                value_new = -100 - less_than_100
             else:
              value_new = value_new - (value_new * 0.05)
        #round value_new to nearest whole number 
        value_new = round(value_new)
        return value_new
       
       def decimal_to_american(decimal_odds):
        american_odds = np.where(decimal_odds >= 2.0, (decimal_odds - 1) * 100, -100 / (decimal_odds - 1))
        return american_odds.astype(int)
        
       filtered_df = None

       try:
          session = self.db_manager.create_session()
          engine = self.db_manager.get_engine()
          query = """
                    SELECT *
                    FROM master_model_observations
                    WHERE sport_title IN ('MLB', 'NHL', 'NBA')
                    AND completed = False;
                    """
          filtered_df = pd.read_sql_query(query, engine)
       except Exception as e:
        print(e)
        return str(e)
       finally:
            if session:
                session.close()

      
      #  filtered_df = filtered_df[filtered_df['completed'] == False]

       filtered_df = filtered_df[filtered_df['minutes_since_commence'] > 0]


       filtered_df = filtered_df.dropna(subset=['team'])

       filtered_df = filtered_df.dropna(subset=['opponent'])

       filtered_df = filtered_df[filtered_df['team'].astype(str).str.strip() != '']
        
       filtered_df.sort_values(by="highest_bettable_odds", ascending=False, inplace=True)

       filtered_df = filtered_df.groupby('game_id').first()

       filtered_df.sort_values(by="snapshot_time", ascending=False, inplace=True)

       columns_to_compare = ['team']

       df_no_duplicates = filtered_df.drop_duplicates(subset=columns_to_compare)

        # Apply the conversion function using NumPy vectorization
       
       df_no_duplicates['highest_bettable_odds'] = decimal_to_american(df_no_duplicates['highest_bettable_odds'])
       
       first_20_rows = df_no_duplicates.head(20)

       if 'team' in first_20_rows.columns:
          first_20_rows['team_1'] = first_20_rows['team']
       else:
          pass
       
       if not first_20_rows.empty:
        first_20_rows['highest_acceptable_odds']= first_20_rows.apply(calculate_accepted_bettable_odds, axis=1)

       current_time = datetime.now() 

       first_20_rows['current_time'] = np.where(first_20_rows['sport_title'] == "NBA_PREGAME", current_time + pd.Timedelta(hours=2), current_time)

       first_20_rows['snapshot_time'].apply(pd.to_datetime)

       first_20_rows['current_time'] = pd.to_datetime(first_20_rows['current_time'])

       first_20_rows['snapshot_time'] = pd.to_datetime(first_20_rows['snapshot_time'])

        # Format the datetime object as "Thu, Nov 9, 2023"
       first_20_rows['game_date'] = pd.to_datetime(first_20_rows['game_date']).dt.strftime("%a %b %d, %Y")

       first_20_rows['time_difference_seconds'] = (first_20_rows['current_time'] - first_20_rows['snapshot_time']).dt.total_seconds()

       first_20_rows['time_difference_seconds'] = first_20_rows['time_difference_seconds'] -21600

       first_20_rows['sportsbooks_used'] = first_20_rows['sportsbooks_used'].apply(ast.literal_eval)

       first_20_rows['sportsbooks_used'] = first_20_rows['sportsbooks_used'].apply(lambda x: format_list_of_strings([x]))
       
       first_20_rows = first_20_rows.apply(minutes_seconds, axis=1)

       first_20_rows = self.get_recommended_bet_size(bankroll, first_20_rows)
       
       first_20_rows['ev'] = first_20_rows['ev'].round(1)

       return first_20_rows


    def get_positive_ev_dash_data_react(self, user_bankroll, df, username):
       
       
       def calculate_bet_amount(row, user_bankroll, kelley_criterion):
          user_bankroll = float(user_bankroll)
          odds = row['highest_bettable_odds'] 
          win_probability = row['no_vig_prob_1']
          kelly_percentage = ((win_probability * odds) - 1) / (odds-1)
          return float(kelly_percentage) * float(user_bankroll) * float(kelley_criterion)
       
       def minutes_seconds(row):
          seconds = int(float(row['time_difference_seconds']))
          if seconds < 60:
            row['time_difference_formatted'] = f'{seconds} sec'

          elif seconds >= 60 and seconds < 3600:
            minutes = math.floor(seconds / 60)
            new_seconds = (seconds % 60)
            row['time_difference_formatted'] = f'{minutes} min {new_seconds} sec'
             
          else:
            hours = math.floor(seconds / 3600)
            seconds_after_hour = seconds % 3600
            new_minutes = math.floor(seconds_after_hour / 60)
            new_seconds = seconds_after_hour % 60
            row['time_difference_formatted'] = f'{hours} hours {new_minutes} min {new_seconds} sec'
          
          return row
      
       df = df[df['highest_bettable_odds'] > 1.01]

       df['game_date'] = pd.to_datetime(df['game_date'])

       # If the timestamps are already timezone-aware, use tz_convert directly
       df['game_date'] = df['game_date'].dt.tz_convert('GMT')

       current_time_gmt = datetime.now(pytz.timezone('GMT'))
       df = df[df['game_date'] >= current_time_gmt]

       df['game_date']= (df['game_date'] - pd.Timedelta(hours=6)).dt.strftime('%A, %B %d, %Y')
     
       columns = [col for col in df.columns if col != 'sportsbooks_used']
    
       df_no_duplicates = df.drop_duplicates(subset=columns)
       
       if username is not None:
        try:
          session = self.db_manager.create_session()
          user = session.query(LoginInfo).filter_by(username=username).first()
          logger.info(f'6 {datetime.now()},  memory: {process.memory_info().rss / 1024 / 102}')


          if user:
              kelley_criterion = user.kelley_criterion
              
        except Exception as e:
              logger.error(f"Error getting kelley {str(e)}")
              session.rollback()
       
        finally:
          session.close()
       else:
          kelley_criterion = 0.25

       df_no_duplicates['bet_amount'] = df_no_duplicates.apply(calculate_bet_amount, args=(user_bankroll,kelley_criterion,), axis=1)


       df_no_duplicates['bet_amount'] = df_no_duplicates['bet_amount'].round(2)

       first_20_rows = df_no_duplicates
       

       current_time = datetime.now() 

       first_20_rows['current_time'] = current_time

       first_20_rows['snapshot_time'].apply(pd.to_datetime)
       
       eastern = timezone('US/Eastern')
       current_timeET = datetime.now(eastern)

       is_night_time = current_timeET.time() >= datetime.strptime('01:00', '%H:%M').time() and current_timeET.time() <= datetime.strptime('08:00', '%H:%M').time()

# Set the time difference threshold based on the time of day
       if is_night_time:
          time_threshold_seconds = 3900 
          logger.info('1 hour and 5 minute threshold') # 1 hour and 5 minutes = 3900 seconds
       else:
          logger.info('5 minute threshold')
          time_threshold_seconds = 300  # 5 minutes = 300 seconds






       first_20_rows['current_time'] = pd.to_datetime(first_20_rows['current_time'])

       first_20_rows['snapshot_time'] = pd.to_datetime(first_20_rows['snapshot_time'])
       first_20_rows['game_date'] = pd.to_datetime(first_20_rows['game_date']).dt.strftime('%a %b %d, %Y')

       first_20_rows['time_difference_seconds'] = (first_20_rows['current_time'] - first_20_rows['snapshot_time']).dt.total_seconds()
       first_20_rows = first_20_rows[first_20_rows['time_difference_seconds'] < time_threshold_seconds]

      #  first_20_rows = first_20_rows.swifter.apply(minutes_seconds, axis=1)
       
       def vectorized_minutes_seconds(df):
        seconds = df['time_difference_seconds'].astype(int)
        
        # Calculate hours, minutes, and remaining seconds
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        new_seconds = seconds % 60
        
        # Create conditions for different time formats
        condition_seconds = seconds < 60
        condition_minutes = (seconds >= 60) & (seconds < 3600)
        condition_hours = seconds >= 3600
        
        # Create formatted strings for each condition
        format_seconds = new_seconds.astype(str) + ' sec'
        format_minutes = minutes.astype(str) + ' min ' + new_seconds.astype(str) + ' sec'
        format_hours = (hours.astype(str) + ' hours ' + 
                        minutes.astype(str) + ' min ' + 
                        new_seconds.astype(str) + ' sec')
        
        # Use numpy.select to choose the appropriate format based on conditions
        df['time_difference_formatted'] = np.select(
            [condition_seconds, condition_minutes, condition_hours],
            [format_seconds, format_minutes, format_hours]
        )
        
        return df

# Apply the vectorized function to your DataFrame
       first_20_rows = vectorized_minutes_seconds(first_20_rows)
       logger.info(f'6 {datetime.now()}, memory: {process.memory_info().rss / 1024 / 102}')

       return first_20_rows


    def get_ai_ev_dash_data_react(self, user_bankroll, df, username):
       
       def calculate_bet_amount(row, user_bankroll, kelley_criterion):
          user_bankroll = float(user_bankroll)
          odds = row['highest_bettable_odds'] 
          win_probability = row['no_vig_prob_1']
          kelly_percentage = ((win_probability * odds) - 1) / (odds-1)
          return float(kelly_percentage) * float(user_bankroll) * float(kelley_criterion)
       

       def minutes_seconds(row):
          seconds = int(float(row['time_difference_seconds']))
          if seconds < 60:
            row['time_difference_formatted'] = f'{seconds} sec'

          elif seconds >= 60 and seconds < 3600:
            minutes = math.floor(seconds / 60)
            new_seconds = (seconds % 60)
            row['time_difference_formatted'] = f'{minutes} min {new_seconds} sec'
             
          else:
            hours = math.floor(seconds / 3600)
            seconds_after_hour = seconds % 3600
            new_minutes = math.floor(seconds_after_hour / 60)
            new_seconds = seconds_after_hour % 60
            row['time_difference_formatted'] = f'{hours} hours {new_minutes} min {new_seconds} sec'
          
          return row


       df = df[df['highest_bettable_odds'] > 1.01]

       df['game_date'] = pd.to_datetime(df['commence_time'])

       df['game_date'] = df['game_date'].dt.tz_localize('UTC')

       df['game_date'] = df['game_date'].dt.tz_convert('GMT')

       current_time_gmt = datetime.now(pytz.timezone('GMT'))

       df = df[df['game_date'] >= current_time_gmt]

       df['game_date']= (df['game_date'] - pd.Timedelta(hours=6)).dt.strftime('%A, %B %d, %Y')
     
       columns = [col for col in df.columns if col != 'sportsbooks_used_other' and col != 'sportsbooks_used_other_other' and col !='sportsbooks_used']
    
       df_no_duplicates = df.drop_duplicates(subset=columns)

       if username is not None:
        try:
          session = self.db_manager.create_session()
          user = session.query(LoginInfo).filter_by(username=username).first()
          logger.info(f'6 {datetime.now()},  memory: {process.memory_info().rss / 1024 / 102}')


          if user:
              kelley_criterion = user.kelley_criterion
              
        except Exception as e:
              logger.error(f"Error getting kelley {str(e)}")
              session.rollback()
       
        finally:
          session.close()
       
       else:
          kelley_criterion = 0.25

       df_no_duplicates['bet_amount'] = df_no_duplicates.apply(calculate_bet_amount, args=(user_bankroll,kelley_criterion,), axis=1)

       df_no_duplicates['bet_amount'] = df_no_duplicates['bet_amount'].round(2)

       first_20_rows = df_no_duplicates
       
       current_time = datetime.now() 

       first_20_rows['current_time'] = current_time

       first_20_rows['current_time'] = pd.to_datetime(first_20_rows['current_time'])

       first_20_rows['snapshot_time'] = pd.to_datetime(first_20_rows['time_pulled'].astype(int) / 1000, unit='s')

       first_20_rows['game_date'] = pd.to_datetime(first_20_rows['game_date']).dt.strftime('%a %b %d, %Y')

       first_20_rows['current_time'] = pd.to_datetime(first_20_rows['current_time'])
       first_20_rows['snapshot_time'] = pd.to_datetime(first_20_rows['snapshot_time'])

       first_20_rows['time_difference_seconds'] = (first_20_rows['current_time'] - first_20_rows['snapshot_time']).dt.total_seconds()

      #  first_20_rows = first_20_rows[first_20_rows['time_difference_seconds'] < 300]

      #  logger.info(f"After time difference drop: {len(first_20_rows)}")


      #  first_20_rows = first_20_rows.swifter.apply(minutes_seconds, axis=1)
       
       def vectorized_minutes_seconds(df):
        seconds = df['time_difference_seconds'].astype(int)
        
        # Calculate hours, minutes, and remaining seconds
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        new_seconds = seconds % 60
        
        # Create conditions for different time formats
        condition_seconds = seconds < 60
        condition_minutes = (seconds >= 60) & (seconds < 3600)
        condition_hours = seconds >= 3600
        
        # Create formatted strings for each condition
        format_seconds = new_seconds.astype(str) + ' sec'
        format_minutes = minutes.astype(str) + ' min ' + new_seconds.astype(str) + ' sec'
        format_hours = (hours.astype(str) + ' hours ' + 
                        minutes.astype(str) + ' min ' + 
                        new_seconds.astype(str) + ' sec')
        
        # Use numpy.select to choose the appropriate format based on conditions
        df['time_difference_formatted'] = np.select(
            [condition_seconds, condition_minutes, condition_hours],
            [format_seconds, format_minutes, format_hours]
        )
        
        return df

      # Apply the vectorized function to your DataFrame
       first_20_rows = vectorized_minutes_seconds(first_20_rows)
       logger.info(f'6 {datetime.now()}, memory: {process.memory_info().rss / 1024 / 102}')

       return first_20_rows

    def get_arbitrage_dash_data_react(self, df):
       
       def minutes_seconds(row):
          seconds = int(float(row['time_difference_seconds']))
          if seconds < 60:
            row['time_difference_formatted'] = f'{seconds} sec'

          elif seconds >= 60 and seconds < 3600:
            minutes = math.floor(seconds / 60)
            new_seconds = (seconds % 60)
            row['time_difference_formatted'] = f'{minutes} min {new_seconds} sec'
             
          else:
            hours = math.floor(seconds / 3600)
            seconds_after_hour = seconds % 3600
            new_minutes = math.floor(seconds_after_hour / 60)
            new_seconds = seconds_after_hour % 60
            row['time_difference_formatted'] = f'{hours} hours {new_minutes} min {new_seconds} sec'
          
          return row

       def format_list_of_strings(strings):
           return ', '.join(strings[0])

       def decimal_to_american(decimal_odds):
        american_odds = np.where(decimal_odds >= 2.0, (decimal_odds - 1) * 100, -100 / (decimal_odds - 1))
        return american_odds.astype(int)  
       
       def safe_literal_eval(value):
        try:
            return ast.literal_eval(value)
        except (ValueError, SyntaxError):
            return value

       df = df[df['highest_bettable_odds'] > 1.01]

       df['game_date'] = pd.to_datetime(df['game_date'])
       df['game_date'] = df['game_date'].dt.tz_localize(None)

       df['game_date'] = df['game_date'].dt.tz_localize('UTC')
       df['game_date'] = df['game_date'].dt.tz_convert('GMT')

       current_time_gmt = datetime.now(pytz.timezone('GMT'))

       df = df[df['game_date'] >= current_time_gmt]

       df['game_date']= (df['game_date'] - pd.Timedelta(hours=6)).dt.strftime('%A, %B %d, %Y')

       df['sportsbooks_used_formatted'] = df['sportsbooks_used'].apply(lambda x: literal_eval(x) if isinstance(x, str) else x)

       df.drop(columns=['sportsbooks_used_formatted'], inplace=True)

       columns = [col for col in df.columns if  not 'sportsbooks_used' in col]
    
       df_no_duplicates = df.drop_duplicates(subset=columns)

       df_no_duplicates['bet_amount'] = 0

       df_no_duplicates['bet_amount'] = df_no_duplicates['bet_amount'].round(2)

      # Apply the conversion function using NumPy vectorization
       
       df_no_duplicates['highest_bettable_odds_dec'] = df_no_duplicates['highest_bettable_odds'].copy()

       df_no_duplicates['highest_bettable_odds'] = decimal_to_american(df_no_duplicates['highest_bettable_odds'])

       first_20_rows = df_no_duplicates

       if 'team' in first_20_rows.columns:
          first_20_rows['team_1'] = first_20_rows['team']
       else:
          pass
       
       if not first_20_rows.empty:
        first_20_rows['highest_acceptable_odds']= 0
      
       current_time = datetime.now() 

       first_20_rows['current_time'] = current_time #- pd.Timedelta(hours=7)

       first_20_rows['snapshot_time'].apply(pd.to_datetime)

       first_20_rows['current_time'] = pd.to_datetime(first_20_rows['current_time'])

       first_20_rows['snapshot_time'] = pd.to_datetime(first_20_rows['snapshot_time'])

       first_20_rows['game_date'] = first_20_rows['game_date'].apply(lambda x: pd.to_datetime(x).strftime('%a %b %d, %Y'))

       first_20_rows['time_difference_seconds'] = (first_20_rows['current_time'] - first_20_rows['snapshot_time']).dt.total_seconds()

       first_20_rows = first_20_rows[first_20_rows['time_difference_seconds'] < 300]

       first_20_rows['sportsbooks_used'] = first_20_rows['sportsbooks_used'].apply(safe_literal_eval)

       first_20_rows['sportsbooks_used_other'] = first_20_rows['sportsbooks_used_other'].apply(safe_literal_eval)

       first_20_rows['sportsbooks_used'] = first_20_rows['sportsbooks_used'].apply(lambda x: format_list_of_strings([x]))

       first_20_rows['sportsbooks_used_other'] = first_20_rows['sportsbooks_used_other'].apply(lambda x: format_list_of_strings([x]))
       
       first_20_rows = first_20_rows.apply(minutes_seconds, axis=1)

       first_20_rows['ev'] = round(df['arb_perc'] * 100, 2)

       first_20_rows['groupby'] = first_20_rows['game_id'] + first_20_rows['wager_display']

       return_df = first_20_rows.groupby('groupby').first().reset_index()

       return return_df
   

    def check_payments(self, username):

      response = self.get_permission(username)
      if response['permission'] != 'free' and response['permission'] != 'payment_failed' and response['status'] != 'none':
         is_active = 1
      else:
         is_active = 0
      try:
        session = self.db_manager.create_session()
        session.query(LoginInfo).filter_by(username=username).update({"payed": int(is_active)})
        session.commit()
      except Exception as e:
        print(e)
        return str(e)
      finally:
        session.close()

    def get_user_info(self, username):
      user_dict = None
      try:
        # Create a session
        session = self.db_manager.create_session()

        # Query the user's record by username
        user_info = session.query(LoginInfo).filter_by(username=username).first()
        user_dict = {
          "firstname": user_info.firstname,
          "lastname": user_info.lastname,
          "username": user_info.username,
          "password": user_info.password,
          "phone": user_info.phone,
          "bankroll": user_info.bankroll,
          "payed": user_info.payed,
          "date_signed_up": user_info.date_signed_up
        }
      except Exception as e:
        return str(e)
      finally:
        session.close()
        return user_dict


    def cancel_subscription(self,username):
      try:
        # Get the user's subscription ID from the Stripe API
        customer = stripe.Customer.list(email=username)
        subscriptions = stripe.Subscription.list(customer=customer.data[0].id)
        
        if subscriptions.data:
            subscription_id = subscriptions.data[0].id

            # Cancel the subscription in Stripe
            canceled_subscription = stripe.Subscription.delete(subscription_id)

            if canceled_subscription.status == 'canceled':
                # Update the 'paid' column in the SQLite database
              try:
       
                session = self.db_manager.create_session()
                session.query(LoginInfo).filter_by(username=username).update({"payed": 0})
                session.commit()
              except Exception as e:
                print(e)
                return str(e)
              finally:
                session.close()
                return True, "Subscription canceled successfully."
    
        return False, "No active subscription found for this user."
      except stripe.error.StripeError as e:
        print(e)
        return False, str(e)

 
    def read_cached_df(self, path):
      serialized_df = redis_client.get(path)
      if serialized_df:
        serialized_df = serialized_df.decode('utf-8')
        cached_df = pd.read_json(serialized_df)
        return cached_df
      else:
        raise FileNotFoundError(f"No cached DataFrame found at path: {path}")
    

    def generate_secure_token(self):
      token = secrets.token_urlsafe(16)  # Generate a random 16-byte token
      salt = secrets.token_urlsafe(16)  # Generate a random salt
      hash_obj = hashlib.sha256()
      hash_obj.update(token.encode('utf-8') + salt.encode('utf-8'))
      hashed_token = hash_obj.hexdigest()
      return hashed_token
    

    def store_remember_token(self, username, remember_token):
      expiration_timestamp = datetime.utcnow() + timedelta(days=10)

    # Create a new RememberToken instance
      new_token = RememberToken(
        username=username,
        remember_token=remember_token,
        expiration_timestamp=expiration_timestamp
      )
      try:
        session = self.db_manager.create_session()
    # Add the new token to the database session and commit the changes
        session.add(new_token)
        session.commit()
      except Exception as e:
        logger.info(str(e))
        print(e)
        session.rollback()
        return str(e)
      finally:
        session.close()
        return
    

    def get_username_by_remember_token(self, remember_token):
      try:
            session = self.db_manager.create_session()
            token_data = session.query(RememberToken).filter_by(remember_token=remember_token).first()
            if token_data:
              return token_data.username
            else:
              return None
      except Exception as e:
            print(e)
            return str(e)
      finally:
            session.close()

    


    def update_customer_emails(self):
      has_more = True
      starting_after = None
      

      while has_more:
          # Fetch a batch of customers
          customers = stripe.Customer.list(limit=100, starting_after=starting_after)


          for customer in customers.data:
              email = customer.email
              if email and email != email.lower():
                  print(customer.email)
                  try:
                      # Update the customer's email to be lowercase
                      stripe.Customer.modify(customer.id, email=email.lower())
                      print(f"Updated email for customer {customer.id}: {email} -> {email.lower()}")
                  except Exception as e:
                      print(f"Failed to update email for customer {customer.id}: {str(e)}")
          
          # Check if there are more customers to fetch
          has_more = customers.has_more
          if has_more:
              starting_after = customers.data[-1].id
          else:
            break 
          


      print('done with update customer emails')
    

    def decimal_to_float(self,obj):
      if isinstance(obj, Decimal):
          return float(obj)
      return obj  # Let json.dumps handle other types

   
    def get_mma_data(self):
      session = self.db_manager.create_session()
  
      
      try:
        today = func.current_date()
        one_day_ago = func.now() - timedelta(days=1)

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
            .where(latest_odds.game_date >= today, latest_odds.market_key.in_(['h2h']))
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
                    'home_team': mma_odds.home_team,
                    'away_team': mma_odds.away_team,
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
                    'other_home_team': other_row.home_team,
                    'other_away_team': other_row.away_team,
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
            .filter(latest_odds2.game_date >= today)
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
                'home_team': mma_odds.home_team,
                'away_team': mma_odds.away_team,
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

    

    def get_MMA_game_data(self, gameId):

      with self.db_manager.create_session() as session:
        today = func.current_date()
        one_day_ago = func.now() - timedelta(days=1)

        # Subquery to get the most recent pulled_time for each game_id and market
        latest_odds = (
            select(
                MMAOdds.game_id,
                MMAOdds.market,
                func.max(MMAOdds.pulled_time).label('max_pulled_time')
            )
            .filter(and_(
                MMAOdds.game_date >= today,
                MMAOdds.pulled_time >= one_day_ago,
                MMAOdds.game_id == gameId
            ))
            .group_by(MMAOdds.game_id, MMAOdds.market)
            .subquery()
        )
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
            .order_by(MMAOdds.id)
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
   