import requests
import os
import pandas as pd
import numpy as np
from db_manager import DBManager
from models import VerificationCode, RememberToken  # Import your SQLAlchemy models
from datetime import datetime, timedelta

if __name__ == '__main__':
    db_manager = DBManager()

    while True:
        session = db_manager.create_session()
    # Assuming you have SQLAlchemy configured with your database engine and models

        try:
            # Define time thresholds
            current_time = datetime.now()
            one_hour_ago = current_time - timedelta(hours=1)
            three_days_ago = current_time - timedelta(days=3)

            # Delete verification codes older than 1 hour
            session.query(VerificationCode).filter(VerificationCode.time_allowed < one_hour_ago).delete()

            # Delete remember tokens older than 14 days
            session.query(RememberToken).filter(RememberToken.expiration_timestamp < three_days_ago).delete()

            # Commit the changes
            session.commit()

            # Print success message or perform other actions
            print("Rows deleted successfully.")

        except Exception as e:
            # Handle exceptions
            print("Error:", e)
            session.rollback()

        finally:
            # Close the session
            session.close()
