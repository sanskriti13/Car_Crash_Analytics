from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd
import os


def create_spark_session(app_name="MySession"):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark



def read_csv_to_dataframe(file_path, spark_session):

    # Read CSV into DataFrame
    df = spark_session.read.csv(file_path, header=True, inferSchema=True)
    return df

from pyspark.sql import DataFrame

def drop_duplicates_from_dataframe(input_df: DataFrame) -> DataFrame:
    # Drop duplicates
    result_df = input_df.dropDuplicates()

    return result_df



def apply_filters(data_frame, filters):
    filtered_df = data_frame
    
    # Apply each filter
    for condition in filters:
        column_name = condition['column']
        operation = condition['operation']
        filter_value = condition['value']
        
        # Apply filter
        if operation == '==':
            filtered_df = filtered_df.filter(col(column_name) == filter_value)
        elif operation == '<>':
            filtered_df = filtered_df.filter(col(column_name) != filter_value)
        elif operation == '>':
            filtered_df = filtered_df.filter(col(column_name) > filter_value)
        elif operation == '<':
            filtered_df = filtered_df.filter(col(column_name) < filter_value)
        elif operation == 'like':
            filtered_df = filtered_df.filter(col(column_name).like(filter_value))
        elif operation == 'rlike':
            filtered_df = filtered_df.filter(col(column_name).rlike(filter_value))
        elif operation == 'isNotNull':
            filtered_df = filtered_df.filter(col(column_name).isNotNull())

    
    return filtered_df



def apply_join(df1, df2, join_type, join_conditions,select_columns):

    join_condition = None


    for condition in join_conditions:
        left_col = condition['left_col']
        right_col = condition['right_col']
        join_condition = col(left_col) == col(right_col) if join_condition is None else (join_condition) & (col(left_col) == col(right_col))

    # Apply join
    joint_df = df1.alias('l').join(df2.alias('r'), join_condition, how=join_type).select(*select_columns)

    return joint_df



from pyspark.sql import functions as F

def perform_grouping(dataframe, grouping_params):
    grouped_df = dataframe

    for params in grouping_params:
        group_by_cols = params['group_by_cols']
        agg_column = params['agg_column']
        agg_operation = params['agg_operation']

        if agg_operation == 'count':
            grouped_df = grouped_df.groupBy(*group_by_cols).agg(F.count(agg_column).alias(f'{agg_operation}({agg_column})'))
        elif agg_operation == 'distinct_count':
            grouped_df = grouped_df.groupBy(*group_by_cols).agg(F.countDistinct(agg_column).alias(f'{agg_operation}({agg_column})'))
        elif agg_operation == 'sum':
            grouped_df = grouped_df.groupBy(*group_by_cols).agg(F.sum(agg_column).alias(f'{agg_operation}({agg_column})'))
        

    return grouped_df

def dataframe_count(result_df):
    return result_df.count()


def grouping_list_parameters(grouping_params_json):
    grouping_params = []

# Convert the grouping_params to the desired format
    for param in grouping_params_json:
        
        if param["group_by_cols"] =="[]":
            group_by_cols=[]
        else:
            group_by_cols_str = param["group_by_cols"].strip("[]").replace("'", "")
            group_by_cols = [col.strip() for col in group_by_cols_str.split(',')]
        
        agg_column = param["agg_column"].strip("'")
        agg_operation = param["agg_operation"].strip("'")
        
        grouping_params.append({
            "group_by_cols": group_by_cols,
            "agg_column": agg_column,
            "agg_operation": agg_operation
        }) 
    return grouping_params
    
def write_df_to_csv(df,output_path=os.getcwd()):
    if isinstance(df,DataFrame):
        pandas_df = df.toPandas()
    else :
        pandas_df = pd.DataFrame({'Value': [df]})
    pandas_df.to_csv(output_path, index=False)
    





  


