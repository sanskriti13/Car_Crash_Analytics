from funs import *
from analytics_funs import *
import argparse
import json
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
import os,sys


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

args = None

def parse_arguments():
    global args

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--config-json",
        help="json file to be uploaded",
        required=False
    )
    args = parser.parse_args()

def main():
  
  spark =create_spark_session()
  parse_arguments() 
  
  
  with open(str(args.config_json)) as f:
    config_file = json.load(f)
    print("Hi")
    
    # List of all keys
    active_flag_keys = [key for key, value in config_file.items() if value.get("active_flag") == "Y"]
    
    output_path="D:\\python\\Car_Crash\\Car_Crash_Analytics"
    
    for i in active_flag_keys:
      print(i)
      analytics= eval(i+("_fun"))(config_file.get(i),spark)
      if isinstance(analytics, DataFrame):
        analytics.show(truncate=False)
      else:
        print(analytics)
      try:
        write_df_to_csv(analytics,output_path)
      except Exception as e:
        print("Couldn't load DF",e)
        
        
        
    # print(config_file.get("analytics_1"))  
    # analytics_1 = analytics1(config_file.get("analytics_1"),spark)
    # print(analytics_1)
    
    
    # print(config_file.get("analytics_2"))  
    # analytics_2 = analytics2(config_file.get("analytics_2"),spark)
    # print(analytics_2)

    # print(config_file.get("analytics_3"))  
    # analytics_3 = analytics3(config_file.get("analytics_3"),spark)
    # analytics_3.show(truncate=False)
    
    # print(config_file.get("analytics_4"))  
    # analytics_4 = analytics4(config_file.get("analytics_4"),spark)
    # print(analytics_4)
    
    
    # print(config_file.get("analytics_5"))  
    # analytics_5 = analytics5(config_file.get("analytics_5"),spark)
    # print(analytics_5)
    
    # print(config_file.get("analytics_6"))  
    # analytics_6 = analytics6(config_file.get("analytics_6"),spark)
    # analytics_6.show(truncate=False)
    
    # print(config_file.get("analytics_7"))  
    # analytics_7 = analytics7(config_file.get("analytics_7"),spark)
    # analytics_7.show(truncate=False)
    
    # print(config_file.get("analytics_8"))  
    # analytics_8 = analytics8(config_file.get("analytics_8"),spark)
    # analytics_8.show(truncate=False)
    
    
  
  
  
  
  spark.stop()
  
  
  
  
  


main()