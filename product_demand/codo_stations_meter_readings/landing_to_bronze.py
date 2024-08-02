# Databricks notebook source
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from pyspark.sql.functions import  col, upper,lit ,date_format ,input_file_name
from datetime import datetime, timedelta
from lib.utils import generate_date

import time

# COMMAND ----------

#Set up credentials
credential = DefaultAzureCredential()
secret_client = SecretClient(vault_url="https://azkeyvault2f2ph6.vault.azure.net/", credential=credential)

storage_account_name = "azwksdatalake2f2ph6.blob.core.windows.net"
container_name = "azsynapsewks2f2ph6"
synapse_temp_dir = f"wasbs://{container_name}@{storage_account_name}/synapse/temp/"
config_path = f"wasbs://{container_name}@{storage_account_name}/config/"

# COMMAND ----------

#Retrieve storage credentals

storage_account_key = secret_client.get_secret("storage-account-key").value
storage_sas_token = secret_client.get_secret("sas-token").value


# COMMAND ----------

# Set up configs
spark.conf.set(
f"fs.azure.account.key.{storage_account_name}", f"{storage_account_key}"
)

spark.conf.set(
f"fs.azure.sas.{container_name}.{storage_account_name}",
f"{storage_sas_token}"
)

# COMMAND ----------

current_time = datetime.now()
dbutils.widgets.text("start_date",(current_time-timedelta(1)).strftime("%Y-%m-%d"))
dbutils.widgets.text("end_date",(current_time-timedelta(1)).strftime("%Y-%m-%d"))

start_date_str = dbutils.widgets.get("start_date")
end_date_str = dbutils.widgets.get("end_date")

run_id = int(time.mktime(current_time.timetuple()))
run_date_int = datetime.fromtimestamp(run_id).strftime("%Y%m%d")

start_date = datetime.strptime(start_date_str,"%Y-%m-%d")
end_date = datetime.strptime(end_date_str,"%Y-%m-%d")

print("Start date: " + str(start_date))
print("End date: " + str(end_date))
print("run id: " + str(run_date_int))


# COMMAND ----------

# define paths 
container_var = f"wasbs://{container_name}@{storage_account_name}/"
bronze_path = f"{container_var}bronze/product_demand/codo_station_meter_reading/"


# COMMAND ----------

# load landing paths 
codo_meter_reading_loaded_df_arr = []
dates = generate_date(start_date , end_date,"%Y%m%d")
for day in dates:
    landing_path = f"{container_var}landing/product_demand/codo_station_meter_reading/{day}/codo_station_meter_reading.csv"
    try:
        each_day_df = (spark.read.format('csv')
                            .option('header', True)
                            .option("inferSchema", "true")
                            .option("sep", ",")   
                            .load(landing_path)
                            .withColumn("FILE_NAME", input_file_name()))
        codo_meter_reading_loaded_df_arr.append(each_day_df)
    except:
        print(f"The file {landing_path} does not exist")


# COMMAND ----------

 if (len(codo_meter_reading_loaded_df_arr) > 0):   
    codo_meter_reading_merged_df = codo_meter_reading_loaded_df_arr[0]
    for x in range(len(codo_meter_reading_loaded_df_arr)):
        if(x > 0):
           codo_meter_reading_merged_df = codo_meter_reading_merged_df.unionAll(codo_meter_reading_loaded_df_arr[x])
else :
    dbutils.notebook.exit("No files to load")


# COMMAND ----------

final_codo_station_meter_reading_df = codo_meter_reading_merged_df.select([col(cols).alias(cols.replace(' ','_').upper()) for cols in codo_meter_reading_merged_df.columns])

# COMMAND ----------

#Add water mark columns 
final_codo_station_meter_reading_df = (final_codo_station_meter_reading_df                                      
                      .withColumn("SALES_DATE",date_format("SALES_DATE","yyyyMMdd"))                      
                       .withColumn('RUN_DATE_ID', lit(run_date_int).cast('int'))
                       .withColumn("INSERT_LOAD_TIME_ID", lit(run_id).cast('int'))
                       .withColumn("INSERT_LOAD_TIME", lit(current_time).cast('timestamp')))

# COMMAND ----------

unique_dates = final_codo_station_meter_reading_df.select("SALES_DATE").distinct().rdd.flatMap(lambda x: x).collect()
unique_stations = final_codo_station_meter_reading_df.select("STATION").distinct().rdd.flatMap(lambda x: x).collect()

dates_processed = [] 
for dates_ in unique_dates : 
    dates_processed.append(dates_)

station_name_processed = [] 
for station_name in unique_stations : 
    station_name_processed.append(str(station_name))


# COMMAND ----------

#Create a comma separated list
dates_processed_str = ",".join(str(i) for i in dates_processed)
station_name_processed_str = ",".join(str(i) for i in station_name_processed)

# COMMAND ----------

partition_predicate = f"SALES_DATE IN ({dates_processed_str}) "

# COMMAND ----------

#this is to force caching process
print(final_codo_station_meter_reading_df.count())

# COMMAND ----------

"""WRITTING IN STATION STOCK VOLUME"""
(final_codo_station_meter_reading_df.write.format("delta")
 .partitionBy("SALES_DATE")
 .option("replaceWhere",  f"{partition_predicate}")
.mode("overwrite").save(bronze_path))

final_codo_station_meter_reading_df.unpersist(blocking=True)
