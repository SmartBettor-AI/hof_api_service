from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
import os

# Define your MySQL database parameters
mysql_database = os.environ.get("db_name")
endpoint = os.environ.get("database_endpoint")
username = os.environ.get("db_username")
password = os.environ.get("mysql_pass")

class DBManager:
    def __init__(self):
        mysql_uri = f'mysql+pymysql://{username}:{password}@{endpoint}/{mysql_database}'
        self.engine = create_engine(mysql_uri, pool_pre_ping=True, poolclass=QueuePool)
        self.Session = sessionmaker(bind=self.engine)

    def create_session(self):
        return self.Session()
    def get_engine(self):
        return self.engine

    def close(self):
        self.engine.dispose()

db_manager = DBManager()
