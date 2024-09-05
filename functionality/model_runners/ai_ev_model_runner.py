from ai_ev_mlb_model import MLBRunner
from ai_ev_ncaaf_model import NCAAFRunner
# from result_updater import result_updater
# from ai_model_cacher import AIModelCacher
import time

if __name__ == "__main__":

  mlb_runner = MLBRunner(name = 'MLB_08_23_2024_model_silu_profit_1000e_0_1e-05lr_Falsewd_1000mb', live_or_pregame='pregame', sport='MLB')

  ncaaf_runner = NCAAFRunner(name = 'CFB_08_27_2024_model_relu_profit_100e_32_1e-05lr_Falsewd_500mb', live_or_pregame='pregame', sport='CFB')

  while True:
    try:
      df = mlb_runner.run()
    except Exception as e:
      print(e)

    try:
      df = ncaaf_runner.run()
    except Exception as e:
      print(e)

    time.sleep(10)