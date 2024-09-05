from ai_pregame_model import pregame_ai
from ai_pregame_model_07_18_2024 import pregame_ai_07_18_2024
from result_updater import result_updater
from ai_model_cacher import AIModelCacher
import time

if __name__ == "__main__":

  pregame_ai_07_18_2024 = pregame_ai_07_18_2024(
    name="mlb_07_18_2024_model"
  )

  cacher = AIModelCacher()

  result_updater_instance = result_updater()


  while True:

    pregame_ai_07_18_2024.make_live_dash_data()

    result_updater_instance.update_results('baseball_mlb')

    cacher.perform_cache()

    time.sleep(600)
