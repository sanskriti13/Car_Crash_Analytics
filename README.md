# Car_Crash_Analytics
An application that performs analysis on US Car Crash Data completely using Pyspark (without using Spark SQL) taking multiple csv files as input in a config-based format and storing results in csv files.

How is the code working?
1. User Input :
   a. Analysis needs to be executed or not : Through Active Flag (If Active Flag is Y, the pydspark function for that analytical query will be executed)
   b. User has the option of specifying the patch of the data to be used for the analysis as a part of the Config File
   c. All the variables have been configured by the user in the Config File


2. Once this is done, code execution will be through Spark Submit command :
   **Spark Submit Command Used :** spark-submit --master yarn --deploy-mode client --driver-memory 8g --executor-memory 16g --executor-cores 2 D:\python\Car_Crash\Car_Crash_Analytics\Code\main1.py --config-json "D:\python\Car_Crash\Car_Crash_Analytics\Code\config.json"

3. The spark submit command will take the configuration file as an input & execute the data processing steps for all active analytical queries.
   <img width="772" alt="image" src="https://github.com/sanskriti13/Car_Crash_Analytics/assets/63180433/59959cf6-8c15-447c-b568-2be530dea35f">

   
4. To reduce redundancies and increase reusability, User Defined function for each basic Pyspark Operation/ Functionality have been created:
   a. create_spark_session : Creation of Spark Session
   b. read_csv_to_dataframe : Read data from CSV Files
   c. drop_duplicates_from_dataframe : Drop Duplicate Records from Dataframe
   d. apply_filters : Apply filters on dataframe
   e. apply_join : Apply join between 2 data frames
   f. perform_grouping : Apply grouping operations/ Aggregations on dataframes
   g. dataframe_count : Return Count of Dataframe
   h. write_df_to_csv : Return CSV File with Result Dataframe

5. Problem Statments :
  a. Analytical Functions (analytics_1_fun to analytics_9_fun) have been created that leverage the above functions and some additional operations to perform the required analysis as mentioned in the Problem Statements.
  b. The Main Function : Pulls all the configurations from the Config File provided by the user and returns results for all the





