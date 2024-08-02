# Databricks notebook source
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from pyspark.sql.functions import  col, upper,lit ,date_format, input_file_name
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
bronze_path = f"{container_var}bronze/product_demand/sap_loading/"

# COMMAND ----------

# generate landing paths 
landing_paths = []
dates = generate_date(start_date , end_date,"%Y%m%d")
for day in dates:
    landing_path = f"{container_var}landing/product_demand/sap_loading/{day}/sap_loading_report.csv"
    landing_paths.append(landing_path)


# COMMAND ----------

# load landing paths 
sap_loading_report_array = []
dates = generate_date(start_date , end_date,"%Y%m%d")
for day in dates:
    landing_path = f"{container_var}landing/product_demand/sap_loading/{day}/sap_loading_report.csv"
    try:
        each_day_df = (spark.read.format('csv')
                            .option('header', True)
                            .option("inferSchema", "true")
                            .option("sep", ",")   
                            .load(landing_path)
                            .withColumn("FILE_NAME", input_file_name()))
        sap_loading_report_array.append(each_day_df)
    except:
        print(f"The file {landing_path} does not exist")

# COMMAND ----------

if len(sap_loading_report_array)!= 0:
    sap_loading_merged_df = sap_loading_report_array[0]
    if (len(sap_loading_report_array) >1):
        for x in range(len(sap_loading_report_array)):
            if(x >0):
                sap_loading_merged_df = sap_loading_merged_df.unionAll(sap_loading_report_array[x])
else:
    dbutils.notebook.exit("No files to load")

# COMMAND ----------

sap_loading_report_df = (sap_loading_merged_df.select([col(cols).alias(cols.replace(' ','_').upper()) for cols in sap_loading_merged_df.columns])
                         .withColumn("LOADING_DATE",date_format("LOADING_DATE","yyyyMMdd")))

# COMMAND ----------

#Add water mark columns 
clean_sap_loading_report_df = (sap_loading_report_df
                    .withColumn("LOADING_DATE",sap_loading_report_df.LOADING_DATE.cast('int')) 
                       .withColumn('RUN_DATE_ID', lit(run_date_int).cast('int'))
                       .withColumn("INSERT_LOAD_TIME_ID", lit(run_id).cast('int'))
                       .withColumn("INSERT_LOAD_TIME", lit(current_time).cast('timestamp')))


# COMMAND ----------

clean_sap_loading_report_df.display()

# COMMAND ----------

unique_dates = clean_sap_loading_report_df.select("LOADING_DATE").distinct().rdd.flatMap(lambda x: x).collect()

dates_processed = [] 
for dates_ in unique_dates : 
    dates_processed.append(dates_)

# COMMAND ----------

#Create a comma separated list
dates_processed_str = ",".join(str(i) for i in dates_processed)

# COMMAND ----------

partition_predicate = f"LOADING_DATE IN ({dates_processed_str}) " 

# COMMAND ----------

#this is to force caching process
print(sap_loading_report_df.count())

# COMMAND ----------

"""WRITTING IN STATION STOCK VOLUME"""
(clean_sap_loading_report_df.write.format("delta")
 .partitionBy("LOADING_DATE")
 .option("replaceWhere",  f"{partition_predicate}")
.mode("overwrite").save(bronze_path))

clean_sap_loading_report_df.unpersist(blocking=True)
