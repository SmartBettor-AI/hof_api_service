import requests
import os
import pandas as pd
import numpy as np
# from pos_ev_runner_obj import PositiveEVDashboardRunner
from pos_ev_runner_obj_THREADING import PositiveEVDashboardRunner
import time
from datetime import datetime
from pytz import timezone

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

pd.options.mode.chained_assignment = None
import warnings
warnings.filterwarnings("ignore")

if __name__ == '__main__':
   
   obj = PositiveEVDashboardRunner("CFB")
   def is_between_1am_and_7am_et():
    eastern = timezone('US/Eastern')
    current_time = datetime.now(eastern).time()
    return current_time >= datetime.strptime('01:00', '%H:%M').time() and current_time <= datetime.strptime('06:55', '%H:%M').time()


   while True:
      try:
         obj.make_live_dash_data()
         if is_between_1am_and_7am_et():
                logger.info("Between 1am ET and 7am ET. Sleeping for 1 hour.")
                time.sleep(3600)  # Sleep for 1 hour
      except Exception as e:
         print(e)