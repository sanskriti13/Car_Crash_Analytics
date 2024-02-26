
from funs import *
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas



def analytics_1_fun(dict, spark_session):
  file_path = dict.get('filepath_person_details', '')
  male_filters = dict.get('function_dict', {}).get('male_filters', [])
  grouping_params = grouping_list_parameters(dict["function_dict"]["grouping_params"])
  count_filters = dict.get('function_dict', {}).get('count_filters', [])
  
  person_details_tbl_df= read_csv_to_dataframe(file_path,spark_session)
  
  filtered_df= apply_filters(person_details_tbl_df,male_filters)

  male_count_df = perform_grouping(filtered_df,grouping_params)
  #male_count_df.show(truncate=False)                  
  
  male_count_df_filtered=apply_filters(male_count_df,count_filters)
  
  return dataframe_count(male_count_df_filtered)
  

    



def analytics_2_fun(dict,spark):

  filepath_units_df = dict.get('filepath_units_df', '')
  vehicle_filters = dict.get('function_dict', {}).get('vehicle_filters', [])
  grouping_params = grouping_list_parameters(dict["function_dict"]["grouping_params"])
  units_df = read_csv_to_dataframe(filepath_units_df,spark)
  filtered_df = (
        apply_filters(units_df,vehicle_filters))
    
  result_df= perform_grouping(filtered_df,grouping_params)
  result = result_df.collect()[0]["distinct_count(VIN)"]
  return result




def analytics_3_fun(dict,spark):
  
  filepath_person_details = dict.get('filepath_person_details', '')
  filepath_units_df = dict.get('filepath_units_df', '')
  
  join_conditions = dict.get('function_dict', {}).get('join_conditions', [])
  select_columns=  dict.get('function_dict', {}).get('select_columns', [])
  driver_filters= dict.get('function_dict', {}).get('driver_filters', [])
  
  
  units_df = read_csv_to_dataframe(filepath_units_df,spark)
  person_details_tbl_df= read_csv_to_dataframe(filepath_person_details,spark)

  tbl = apply_filters(apply_join(person_details_tbl_df, units_df, 'inner', join_conditions,select_columns),driver_filters).distinct()
    
  window_spec = Window.orderBy(F.desc('count_distinct_VIN'))

  cte = tbl.groupBy('VEH_MAKE_ID') \
    .agg(F.countDistinct('VIN').alias('count_distinct_VIN')) \
    .withColumn('RANK', F.dense_rank().over(window_spec))


  result = cte.filter('RANK < 6').select('VEH_MAKE_ID')
  return result
  
  
def analytics_4_fun(dict,spark):
  
  filepath_person_details = dict.get('filepath_person_details', '')
  filepath_units_df = dict.get('filepath_units_df', '')
  
  join_conditions = dict.get('function_dict', {}).get('join_conditions', [])
  select_columns=  dict.get('function_dict', {}).get('select_columns', [])
  driver_filters= dict.get('function_dict', {}).get('driver_filters', [])
  grouping_params = grouping_list_parameters(dict["function_dict"]["grouping_params"])
  
  
  units_df = read_csv_to_dataframe(filepath_units_df,spark)
  person_details_tbl_df= read_csv_to_dataframe(filepath_person_details,spark)
  
  
  result_df = perform_grouping(apply_filters(apply_join(person_details_tbl_df, units_df, 'inner', join_conditions,select_columns),driver_filters),grouping_params)
  
  
  result = result_df.collect()[0]["distinct_count(VIN)"]
  return result
            
def analytics_5_fun(dict,spark):
  
  filepath_person_details = dict.get('filepath_person_details', '')
  grouping_params = grouping_list_parameters(dict["function_dict"]["grouping_params"])
  person_details_tbl_df= read_csv_to_dataframe(filepath_person_details,spark)
  females_not_involved = (person_details_tbl_df.select("crash_id").distinct()
  .subtract(person_details_tbl_df.filter(col("PRSN_GNDR_ID") == "FEMALE").select("crash_id")))
  
  
  crash_id_list = females_not_involved.select("crash_id").toPandas()["crash_id"].tolist()
  
  df=person_details_tbl_df.filter(col("crash_id").isin(crash_id_list))
  max_state = perform_grouping(df,grouping_params)\
      .orderBy(F.desc("count(crash_id)")).limit(1)


  result_df = max_state.select("DRVR_LIC_STATE_ID")
  result = result_df.collect()[0]["DRVR_LIC_STATE_ID"]
  return result


def analytics_6_fun(dict, spark):

  filepath_units_df = dict.get('filepath_units_df', '')
  units_tbl_df= read_csv_to_dataframe(filepath_units_df,spark)

  
  vehicle_injuries = (units_tbl_df.groupBy("VIN", "VEH_MAKE_ID")
      .agg(
          F.sum(F.when(F.col("TOT_INJRY_CNT") != 0, F.col("TOT_INJRY_CNT")).otherwise(F.col("DEATH_CNT"))).alias("TotalInjuries")
      )
      .orderBy("VIN", "VEH_MAKE_ID")
  )

  
  window_spec = Window.orderBy(F.desc(F.sum("TotalInjuries")))

  injuries_group_vehicle_makers = (
      vehicle_injuries
      .groupBy("VEH_MAKE_ID")
      .agg(
          F.sum("TotalInjuries").alias("TotalInjuries"),
          F.dense_rank().over(window_spec).alias("rank")
      )
  )

  result = (
      injuries_group_vehicle_makers
      .filter("rank between 3 and 5")
      .select("VEH_MAKE_ID")
      .orderBy("rank")
  )


  return result


def analytics_7_fun(dict, spark):
  
  
  filepath_person_details = dict.get('filepath_person_details', '')
  filepath_units_df = dict.get('filepath_units_df', '')
  
  join_conditions = dict.get('function_dict', {}).get('join_conditions', [])
  select_columns=  dict.get('function_dict', {}).get('select_columns', [])
  driver_filters= dict.get('function_dict', {}).get('driver_filters', [])
  
  
  units_df = read_csv_to_dataframe(filepath_units_df,spark)
  units_tbl_df= drop_duplicates_from_dataframe(units_df)
  person_details_tbl_df= read_csv_to_dataframe(filepath_person_details,spark)
  
  cte = (apply_join(person_details_tbl_df,units_tbl_df,'inner',join_conditions,select_columns)
    .groupBy("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")
    .agg(
        F.count("*").alias("count"),
        F.rank().over(Window.partitionBy("VEH_BODY_STYL_ID").orderBy(F.desc(F.count("*")))).alias("rank")
    )
)

  # Select VEH_BODY_STYL_ID, PRSN_ETHNICITY_ID where rank is 1
  
  
  result = (apply_filters(cte,driver_filters)
      .select("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")
  )
  return result



def analytics_8_fun(dict,spark):
  
    filepath_person_details = dict.get('filepath_person_details', '')
    filepath_units_df = dict.get('filepath_units_df', '')
    grouping_params = grouping_list_parameters(dict["function_dict"]["grouping_params"])
    
    
    join_conditions = dict.get('function_dict', {}).get('join_conditions', [])
    select_columns=  dict.get('function_dict', {}).get('select_columns', [])
    driver_filters= dict.get('function_dict', {}).get('driver_filters', [])
    person_details_tbl_df= read_csv_to_dataframe(filepath_person_details,spark)
    units_df = read_csv_to_dataframe(filepath_units_df,spark)
   
   
    joined_df = apply_join(person_details_tbl_df,units_df,'inner',join_conditions,select_columns)
    joined_df.show(truncate=False)
    
    filtered_df = apply_filters(joined_df,driver_filters)

    
    window_spec = Window.orderBy(F.desc('distinct_count(crash_id)'))
    ranked_df = perform_grouping(filtered_df,grouping_params).withColumn('rank', F.rank().over(window_spec))

    # Select result
    result_df = ranked_df.filter(col('rank') < 5).select('OWNR_ZIP')

    return result_df
  
  
  
def analytics_9_fun(dict,spark):

  
  filepath_units_df = dict.get('filepath_units_df', '')
  driver_filters= dict.get('function_dict', {}).get('driver_filters', [])
  units_df = read_csv_to_dataframe(filepath_units_df,spark)
  
  filtered_df = apply_filters(units_df,driver_filters)
  result_count = filtered_df.select("crash_id").distinct().count()

  return result_count