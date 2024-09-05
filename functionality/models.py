from sqlalchemy import Column, Integer, String, Text,Date, Boolean, Float, DateTime, ForeignKey, JSON, Numeric
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
class RememberToken(Base):
    __tablename__ = 'remember_tokens'
    id = Column(Integer, primary_key=True, autoincrement = True)
    username = Column(String(255), nullable=False)
    remember_token = Column(String(255), nullable=False)
    expiration_timestamp = Column(DateTime, nullable=False)



class UserFilters(Base):
    __tablename__ = 'user_filters'

    username = Column(String(255), primary_key=True)
    saved_filters = Column(JSON)
    created_at = Column(DateTime, nullable=False)


class UserArbitrageFilters(Base):
    __tablename__ = 'user_arbitrage_filters'
    username = Column(String(255), primary_key=True)
    saved_filters = Column(JSON)
    created_at = Column(DateTime, nullable=False)

class UserAIEVFilters(Base):
    __tablename__ = 'user_ai_ev_filters'
    username = Column(String(255), primary_key=True)
    saved_filters = Column(JSON)
    created_at = Column(DateTime, nullable=False)


class UserPregameFilters(Base):
    __tablename__ = 'user_pregame_filters'
    username = Column(String(255), primary_key=True)
    saved_filters = Column(JSON)
    created_at = Column(DateTime, nullable=False)
    

class ChatQuestions(Base):
    __tablename__ = 'chat_questions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    question = Column(Text, nullable=True)
    response = Column(Text, nullable=True)
    worked_bool = Column(Boolean, nullable=True)
    full_response = Column(Text, nullable=True)


class VerificationCode(Base):
    __tablename__ = 'verification_codes'

    username = Column(String(255), primary_key=True)
    code = Column(Integer, primary_key=True)
    time_allowed = Column(DateTime, nullable=False)
    used = Column(Boolean, nullable=False)

class MasterModelObservations(Base):
    __tablename__ = 'master_model_observations'
    new_column = Column(String(255), primary_key=True)
    sport_title = Column(String(255))
    completed = Column(Boolean)
    game_id = Column(String(255))
    game_date = Column(String(255))
    team = Column(String(255))
    minutes_since_commence = Column(Float)
    opponent = Column(String(255))
    snapshot_time = Column(String(255))
    ev = Column(Float)
    average_market_odds = Column(Float)
    highest_bettable_odds = Column(Float)
    sportsbooks_used = Column(Text)

class LoginInfo(Base):
    __tablename__ = 'login_info'
    firstname = Column(String(255))
    lastname = Column(String(255))
    username = Column(String(255), unique=True, primary_key=True)
    password = Column(String(255))
    phone = Column(String(255))
    bankroll = Column(String(255))
    payed = Column(Integer)
    date_signed_up = Column(String(255))
    how_heard = Column(String(255))
    referral_name = Column(String(255))
    other_source = Column(String(255))
    unitSize = Column(Numeric(10, 2))
    kelley_criterion = Column(Numeric(10, 2))


class MlbExtraInfo(Base):
    __tablename__ = 'mlb_extra_info'

    date = Column(Integer)
    my_id = Column(String(255), primary_key=True)
    number_of_game_today = Column(Integer)
    day_of_week = Column(String(255))
    away_team = Column(String(255))
    away_team_league = Column(String(255))
    away_team_game_number = Column(Integer)
    home_team = Column(String(255))
    home_team_league = Column(String(255))
    home_team_game_number = Column(Integer)
    day_night = Column(String(255))
    park_id = Column(String(255))

class PlacedBets(Base):
    __tablename__ = 'placed_bets'

    game_id = Column(String(255))
    average_market_odds = Column(String(255))
    team = Column(String(255))
    sportsbooks_used = Column(Text)
    bet_amount = Column(String(255))
    highest_bettable_odds = Column(String(255))
    minimum_acceptable_odds = Column(String(255))
    ev = Column(String(255))
    date = Column(String(255))
    time_difference_formatted = Column(String(255))
    user_name = Column(String(255))
    bet_profit = Column(Float)
    time_placed = Column(String(255), primary_key=True)

class Scores(Base):
    __tablename__ = 'scores'

    game_id = Column(String(255), primary_key=True)
    sport_title = Column(String(255))
    commence_time = Column(String(255))
    home_team = Column(String(255))
    away_team = Column(String(255))
    home_team_score = Column(Integer)
    away_team_score = Column(Integer)
    winning_team = Column(String(255))




class MMAOdds(Base):
    __tablename__ = 'mma_odds_test'

    id = Column(Integer, primary_key=True, autoincrement=True)
    market = Column(String(255))
    odds = Column(JSON)
    class_name = Column(String(255))
    matchup = Column(String(255))
    home_team = Column(String(255))
    away_team = Column(String(255))
    highest_bettable_odds = Column(Numeric(10, 2))
    sportsbooks_used = Column(Text)
    market_key = Column(String(255))
    game_date = Column(Date)
    game_id = Column(Integer)
    pulled_time = Column(DateTime)
    event_id = Column(Integer)
    average_market_odds = Column(Integer)
    market_type = Column(String(255))
    dropdown = Column(Integer)

class MMAGames(Base):
    __tablename__ = 'mma_games_backup'

    id = Column(Integer, primary_key=True, autoincrement=True)
    my_game_id = Column(String(255))

class MMAEvents(Base):
    __tablename__ = 'mma_events_backup'

    id = Column(Integer, primary_key=True, autoincrement=True)
    my_event_id = Column(String(255))


class UserLoginTimes(Base):
    __tablename__ = 'user_login_times'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String(255))
    date = Column(String(255))
    permission = Column(String(255))
