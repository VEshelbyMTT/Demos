# Databricks notebook source
# MAGIC %md
# MAGIC # Demo on structured streaming using tollbooth tutorial
# MAGIC 
# MAGIC This tutorial adopts the similar approach for the stream analytics demo but instead adopts databricks as the consumer. It stores the aggregated data in a data lake. 
# MAGIC 
# MAGIC Designed by Victoria Eshelby for DP-203 data engineering course at Microsoft.
# MAGIC 
# MAGIC 
# MAGIC Pre-requisites:
# MAGIC 
# MAGIC 1. IoT tutorial should be downloaded where there is a valid Azure pass 
# MAGIC     - You can download the event hub, stream analytics, resource group here: 
# MAGIC     - https://docs.microsoft.com/en-us/azure/stream-analytics/stream-analytics-build-an-iot-solution-using-stream-analytics
# MAGIC 2. Databricks Needs to be premium in order to use the secret scope. 
# MAGIC     - [databricks-instance]#secrets/createScope
# MAGIC 3. Key vault needs to be installed for secret key and using your data storage key 
# MAGIC     - You can read more about step 2 and 3 here (https://docs.microsoft.com/en-us/azure/databricks/scenarios/store-secrets-azure-key-vault)
# MAGIC 4. Install on cluster via maven:
# MAGIC     - com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // creating a user name for memory//debugging purposes
# MAGIC 
# MAGIC {val tags = com.databricks.logging.AttributionContext.current.tags
# MAGIC 
# MAGIC //*******************************************
# MAGIC // GET USERNAME AND USERHOME
# MAGIC //*******************************************
# MAGIC 
# MAGIC // Get the user's name
# MAGIC val username = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_USER, java.util.UUID.randomUUID.toString.replace("-", ""))
# MAGIC val userhome = s"dbfs:/user/$username"
# MAGIC 
# MAGIC // Set the user's name and home directory
# MAGIC spark.conf.set("com.databricks.training.username", username)
# MAGIC spark.conf.set("com.databricks.training.userhome", userhome)}
# MAGIC 
# MAGIC print("Success!\n")
# MAGIC 
# MAGIC val username = spark.conf.get("com.databricks.training.username")
# MAGIC val userhome = spark.conf.get("com.databricks.training.userhome")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #Accessing the data
# MAGIC 
# MAGIC Here we install our maven package into our cluster, set our event hubs up, accessing the storage path, and set up a checkpoint path within the DBFS library

# COMMAND ----------

#Step 2: Connect to my event hub namespace - EH entry
prefix = "" #ENTER YOUR PREFIX
Endpoint = f'sb://tollappmltar{prefix}-eventhub.servicebus.windows.net/'# put your endpoint
EntityPath = 'entrystream'
SharedAccessKeyName = 'RootManageSharedAccessKey'
SharedAccessKey = "" # ENTER YOUR KEY FROM EVENT HUB

#for a securer point of access use a key vault and secret key for sharedAccessKey :) 
connectionString_entry = f'Enpoint={Endpoint};EntityPath={EntityPath};SharedAccessKeyName={SharedAccessKeyName};SharedAccessKey={SharedAcessKey}'
ehConfEntry = {}
# For 2.3.15 version and above, the configuration dictionary requires that connection string be encrypted.
ehConfEntry['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString_entry)

#step 3 create a checkpoint path in my dbfs 
username = spark.conf.get("com.databricks.training.username")
userhome = spark.conf.get("com.databricks.training.userhome")
checkpointPath = userhome + "/event-hub/write-checkpoint"
dbutils.fs.rm(checkpointPath,True)


#Step 4: Mount my blob storage

FILE_DIR = "data"
FILE_MOUNT = f"/mnt/{FILE_DIR}" #File directory 
STORAGE_ACCOUNT = f"tollappmltar{prefix}" #Storage account
SOURCE = f"wasbs://{FILE_DIR}@{STORAGE_ACCOUNT}.blob.core.windows.net" 
CONFIG = f"fs.azure.account.key.{STORAGE_ACCOUNT}.blob.core.windows.net"
secret_scope_name = ''#put your secret scope name
keyvault_secret_name = '' #put your secret name for your data storage account

dbutils.fs.mount(
    source = SOURCE,
    mount_point = f'{FILE_MOUNT}',  
  extra_configs = {
      f"fs.azure.account.key.{STORAGE_ACCOUNT}.blob.core.windows.net":dbutils.secrets.get(
          scope = keyvault_secret_name, key = keyvault_secret_name)
  })
#


None # suppress output where possible if directory mounted, you can ignore the remoteException. 


# COMMAND ----------

## EXAMPLE OF STATIC DATA 
REGISTRATION_PATH = dbutils.fs.ls(FILE_MOUNT)[0][0]

reg_schema = T.StructType([
    T.StructField('Expired', T.StringType()),
    T.StructField('LicensePlate', T.StringType()),
    T.StructField('RegistrationId', T.StringType()),

])

registration_df = (
    spark
    .read
    .option('multiline','true')
    .format('org.apache.spark.sql.json')
    .schema(schema=reg_schema)
    .load(REGISTRATION_PATH)
    .withColumn("Expired", F.col("Expired").cast("Boolean"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading the data from event hub
# MAGIC 
# MAGIC As we have two specific event hubs, we first load the data and select the body of the message from the event hub as we're only interested in evaluating the data inside the message. Then, because the message is stored as a json string (see below examples for what a single row represents), we want to explode and transform the data so that we can work within our environment.

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import types as T

#Loading event hub streaming data
incoming_entry_stream = (
    spark
    .readStream
    .format("eventhubs")
    .options(**ehConfEntry)
    .option("checkpointLocation", checkpointPath)
    .load()
    ## lets select body only
    .select(F.col('body')) #selecting the message
    .withColumn("body", F.col("body").cast("string")) # replacing body from binary to string
)



# COMMAND ----------

# MAGIC %md
# MAGIC ## Entry Stream
# MAGIC After extracting the body from my eventhub message, the body ends up in one column with many rows. This row contains the following example:
# MAGIC 
# MAGIC     {
# MAGIC     "EntryTime":"2022-06-01T16:59:33.7859393Z",
# MAGIC     
# MAGIC     "CarModel":{
# MAGIC           "Make":"Kenworth",
# MAGIC           "Model":"T680",
# MAGIC           "VehicleType":2,
# MAGIC           "VehicleWeight":4.32}
# MAGIC      
# MAGIC      "State":"AL",
# MAGIC      "TollAmount":30.0,
# MAGIC      "Tag":549500953,
# MAGIC      "TollId":1,
# MAGIC      "LicensePlate":"RAP 3373"
# MAGIC      
# MAGIC      }

# COMMAND ----------

# now lets focus on transforming the dataset so we can work with it. 

# creating a schema 
entry_schema = T.ArrayType(T.StructType([
    T.StructField('EntryTime', T.TimestampType()),
    T.StructField('CarModel', T.StringType()),
    T.StructField('State', T.StringType()),
    T.StructField('TollAmount', T.FloatType()),
    T.StructField('Tag', T.IntegerType()),
    T.StructField('TollID', T.IntegerType()),
    T.StructField('LicensePlate', T.StringType()),
]))

#cleaning the raw dataset to transform from a message from event hub to the main dataset
entry_stream_clean = (
    incoming_entry_stream
    .withColumn('temp', F.explode(F.from_json('body', schema=entry_schema))) #flattening the json script
    .select(
        F.col('temp.EntryTime').alias('EntryTime'),
        F.col('temp.CarModel').alias('CarModel'),
        F.col('temp.State').alias('State'),
        F.col('temp.TollAmount').alias('TollAmount'),
        F.col('temp.Tag').alias('Tag'),
        F.col('temp.TollID').alias('TollID'),
        F.col('temp.LicensePlate').alias('LicensePlate'),
    )
    .withWatermark('EntryTime', '10 minutes')
)
#entry_stream_clean.display()
    
    

# COMMAND ----------

# MAGIC %md
# MAGIC # Aggregating the data 
# MAGIC 
# MAGIC Now lets start aggregating the data and preparing it for our cosmosDB
# MAGIC 
# MAGIC The IOT event hub asked us to build the following SAQL query via stream analytics were the following: 
# MAGIC 
# MAGIC Counting the number of vehicles in a toll booth for every 3 minutes that passes
# MAGIC ```
# MAGIC SELECT TollId, System.Timestamp AS WindowEnd, COUNT(*) AS Count
# MAGIC INTO CosmosDB
# MAGIC FROM EntryStream TIMESTAMP BY EntryTime
# MAGIC GROUP BY TUMBLINGWINDOW(minute, 3), TollId
# MAGIC ```
# MAGIC lets recreate that using pyspark. Pay attention to the output from the display. Can you see how it changes? 

# COMMAND ----------

n = '3 minutes' #you can play around with N to see the effect it has on the data

count_per_n_window = (
    entry_stream_clean
    .withWatermark("EntryTime", "10 minutes") #discard data that passes a 10 minute threshold from the time of running the stream
    .select(F.col('State'),F.col('EntryTime'),F.col('LicensePlate'), F.col('Tag'))
    .groupby(F.col('State'),F.window('EntryTime', n))
    .agg(
        F.count(F.col('LicensePlate')).alias('count_of_cars')
    )
    .select(F.col('State'),F.col('window.*'),F.col('count_of_cars'))
    .withColumnRenamed('start','window_start') # expanding out the window function so not to create a heirarchy
    .withColumnRenamed('end','window_end')


)
count_per_n_window.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Store back to data lake
# MAGIC Perfect! My query does what is expected and now it's time to put this all in a data lake store. The query below writes to the window with the following output. You can play around with the format, with the output mode and trigger to see what works and what doesn't. 
# MAGIC 
# MAGIC - checks if there is new update of data every 25 seconds then appends new data to the data set 
# MAGIC - checkpoints create a write ahead log in case there's any sensors

# COMMAND ----------

(
    count_per_n_window
    .writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", f"{FILE_MOUNT}/m11_demo/aggregation/entry_{n}/")
    .option("checkpointLocation", f"{FILE_MOUNT}/m11_demo/checkpoint/")
    .trigger(processingTime='25 seconds')
    .start()
)



# COMMAND ----------

#Save my manager from an expensive spark bill ;) 
for s in spark.streams.active:
  s.stop()
