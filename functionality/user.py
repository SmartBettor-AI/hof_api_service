import pandas as pd
from functionality.db_manager import db_manager
from functionality.models import LoginInfo, RememberToken  # Import your SQLAlchemy models
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class User():
    def __init__(self, username):
        self.username = username
        self.password = ''


    def create_user(self, firstname, lastname, username, password, phone, bankroll, sign_up_date, payed,how_heard, referral_name, other_source,  unitSize, kelley_criterion, db_manager):
      # try:
      #     df = pd.read_sql_table('login_info', con=db_manager.get_engine())
      # except Exception as e:
      #   print(e)
      #   return str(e)
      
      #change the column date_signed_up to string

      sign_up_date = str(sign_up_date)



      new_user = LoginInfo(
        firstname = firstname,
        lastname = lastname,
        username = username,
        password = password,
        phone = phone ,
        bankroll = bankroll,
        payed = payed,
        date_signed_up = sign_up_date,
        how_heard = how_heard,
        referral_name = referral_name,
        other_source = other_source,
        unitSize = unitSize,
        kelley_criterion = kelley_criterion
      )
      try:
        session = db_manager.create_session()
    # Add the new token to the database session and commit the changes
        session.add(new_user)
        session.commit()
      except Exception as e:
        logger.info(str(e))
        print(e)
        session.rollback()
        return str(e)
      finally:
        session.close()
        return










      # info_row = pd.DataFrame(
      #   [
      #       [firstname, lastname, username, password, phone, bankroll, payed, sign_up_date, how_heard, referral_name, other_source, unitSize, kelley_criterion,]
      #   ],
      #   columns=['firstname', 'lastname', 'username', 'password', 'phone', 'bankroll', 'payed', 'date_signed_up','how_heard', 'referral_name', 'other_source', 'unitSize', 'kelley_criterion']
      #  )

      # # df.loc[len(df)] = info_row

      # # df['date_signed_up'] = pd.to_datetime(df['date_signed_up'])
      # try:
      #     info_row.to_sql('login_info', con=db_manager.get_engine(), if_exists='append', index=False)
      # except Exception as e:
      #   print(e)
      #   return (str(e))
      # finally:
      #   return 
      

    def add_strategy_to_user(self, username, strategy_name):
      df = pd.read_csv('users/user_strategy_names.csv')
      info_row = [username, strategy_name, False]
      df.loc[len(df)] = info_row
      df.to_csv('users/user_strategy_names.csv', index=False)
    
    def delete_strategy_to_user(username, strategy_name):
      df = pd.read_csv('users/user_strategy_names.csv')
      df = df[~((df['username'] == username) & (df['strategy_name'] == strategy_name))]
      df.to_csv('users/user_strategy_names.csv', index=False)
      return df

    def get_strategies_associated_with_user(self):
      df = pd.read_csv('users/user_strategy_names.csv')
      df = df[df['username'] == self.username]
      strategies = df['strategy_name'].tolist()
      unique_strategies = list(set(strategies))
      return unique_strategies




