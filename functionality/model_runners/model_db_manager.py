from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
import os

mysql_database = os.environ.get("db_name")
mysql_database = "prod"

endpoint = os.environ.get("database_endpoint")
username = os.environ.get("db_username")
username = "admin"
password = os.environ.get("mysql_pass")

class DBManager:
    def __init__(self):
        mysql_uri = f'mysql+pymysql://{username}:{password}@{endpoint}/{mysql_database}'
        print(mysql_uri)
        self.engine = create_engine(mysql_uri, pool_pre_ping=True, poolclass=QueuePool)
        self.Session = sessionmaker(bind=self.engine)
        print(f"Connecting to: {mysql_database}@{endpoint} as {username}")

    def create_session(self):
        return self.Session()
    def get_engine(self):
        return self.engine

    def close(self):
        self.engine.dispose()

# db_manager = DBManager()