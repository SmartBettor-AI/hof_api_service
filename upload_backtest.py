import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import random
from sklearn.preprocessing import StandardScaler, OneHotEncoder
import torch
from torch import nn
from torch.utils.data import DataLoader
from sklearn.metrics import roc_auc_score
import plotly.express as px
import pickle
from datetime import datetime, time
import os 
import warnings
import time
import redis
redis_client = redis.Redis(host='localhost', port=6379, db=0)
from functionality.db_manager import DBManager
import pandas as pd
from sqlalchemy import inspect, types
from sqlalchemy.exc import SQLAlchemyError

warnings.filterwarnings("ignore")

def write_to_sql(df, table_name, engine, if_exists='append'):
    try:
        # Get the table schema
        inspector = inspect(engine)
        columns = inspector.get_columns(table_name)
        
        # Create a dictionary of column names and their SQLAlchemy types
        dtype_dict = {col['name']: col['type'] for col in columns}
        
        # Filter the DataFrame to only include columns that are in the SQL schema
        common_columns = [col for col in df.columns if col in dtype_dict]
        df = df[common_columns]
        
        # Update dtype_dict to only include types for columns that are present in the DataFrame
        dtype_dict = {col: dtype_dict[col] for col in common_columns}
        
        # Function to convert a column to the correct type
        def convert_column(col, sql_type):
            if isinstance(sql_type, types.Integer):
                return pd.to_numeric(col, errors='coerce').astype('Int64')
            elif isinstance(sql_type, types.Float):
                return pd.to_numeric(col, errors='coerce')
            elif isinstance(sql_type, types.String):
                return col.astype(str)
            elif isinstance(sql_type, types.Boolean):
                return col.astype(bool)
            elif isinstance(sql_type, types.DateTime):
                return pd.to_datetime(col, errors='coerce')
            else:
                return col  # If we don't know how to convert, leave it as is

        # Convert each column to the correct type
        for col, sql_type in dtype_dict.items():
            if col in df.columns:
                df[col] = convert_column(df[col], sql_type)

        # Write to SQL
        df.to_sql(table_name, engine, if_exists=if_exists, index=False, dtype=dtype_dict)
        print(f"Data successfully written to table {table_name}")

    except SQLAlchemyError as e:
        print(f"An error occurred while writing to the database: {str(e)}")
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")



if __name__ == "__main__":
    db_manager = DBManager()
    big_df = pd.DataFrame()
    for file in os.listdir('/Users/stefanfeiler/Desktop/backtest_final_4'):
      if file.endswith('.csv'):
          df = pd.read_csv(f'/Users/stefanfeiler/Desktop/backtest_final_4/{file}')
          if not df.empty:
            try:
                df.drop(columns='Unnamed: 0', inplace=True)
            except Exception as e:
                print(f"Couldn't drop column: {e}")
            big_df = pd.concat([big_df, df], ignore_index=True)
            print(len(big_df))
    
    big_df['game_id'] = ''
    big_df.to_csv("/Users/stefanfeiler/Desktop/ev_preds_08_28_2024.csv")

    write_to_sql(table_name='ai_ev_observations_DELETE', df=big_df, engine = db_manager.get_engine())