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
   a.** create_spark_session :** Creation of Spark Session

   b.** read_csv_to_dataframe :** Read data from CSV Files

   c.** drop_duplicates_from_dataframe : **Drop Duplicate Records from Dataframe

   d.** apply_filters : **Apply filters on dataframe

   e. **apply_join : **Apply join between 2 data frames

   f.** perform_grouping :** Apply grouping operations/ Aggregations on dataframes

   g.** dataframe_count : **Return Count of Dataframe

   h. **write_df_to_csv :** Return CSV File with Result Dataframe

5. Problem Statments :
  a. Analytical Functions (analytics_1_fun to analytics_9_fun) have been created that leverage the above functions and some additional operations to perform the required       analysis as mentioned in the Problem Statements.

  b. The Main Function : Pulls all the configurations from the Config File provided by the user and returns results for all the

Screenshots Of Outputs from Console:

   a. Analysis 1 :: Find the number of crashes (accidents) in which number of males killed are greater than 2?

   <img width="503" alt="image" src="https://github.com/sanskriti13/Car_Crash_Analytics/assets/63180433/a5bb0b5a-7381-4b48-895b-faac67d243fd">

   b. Analysis 2 : How many two wheelers are booked for crashes? 

   <img width="314" alt="image" src="https://github.com/sanskriti13/Car_Crash_Analytics/assets/63180433/9053a2f7-d264-4496-863b-f11fd4093633">

   c. Analysis 3 : Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.

   <img width="184" alt="image" src="https://github.com/sanskriti13/Car_Crash_Analytics/assets/63180433/3d022e9b-edae-4674-98ff-760578f5f1bb">

   d. Analysis 4 : Determine number of Vehicles with driver having valid licences involved in hit and run? 

   <img width="99" alt="image" src="https://github.com/sanskriti13/Car_Crash_Analytics/assets/63180433/96555c52-21c4-473f-98c7-13a21503cc62">

   e. Analysis 5: Which state has highest number of accidents in which females are not involved?

   <img width="503" alt="image" src="https://github.com/sanskriti13/Car_Crash_Analytics/assets/63180433/f1b3311e-1457-47a9-a05d-baba1f8c1711">

   f.  Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death

   <img width="599" alt="image" src="https://github.com/sanskriti13/Car_Crash_Analytics/assets/63180433/66cc794d-54ba-47ed-87ca-5709930c8d91">

   g. Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  

   <img width="521" alt="image" src="https://github.com/sanskriti13/Car_Crash_Analytics/assets/63180433/426249c8-0080-42d3-8ef5-dfdd585eb8f3">

   h. Analysis 8: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip             Code)
   <img width="83" alt="image" src="https://github.com/sanskriti13/Car_Crash_Analytics/assets/63180433/e50a3b9d-b64e-4744-883a-f330a98886ed">

   i. Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance

   <img width="135" alt="image" src="https://github.com/sanskriti13/Car_Crash_Analytics/assets/63180433/5b85a562-9477-4602-b079-6408e1eb3923">

   
   










