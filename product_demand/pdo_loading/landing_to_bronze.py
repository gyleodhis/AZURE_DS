# Databricks notebook source
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from pyspark.sql.functions import  col, upper,lit,date_format
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

storage_account_key = secret_client.get_secret("storage-account-key").value
storage_sas_token = secret_client.get_secret("sas-token").value

# COMMAND ----------

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

"""DEFINE PATHS"""
container_var = f"wasbs://{container_name}@{storage_account_name}/"
bronze_path = f"{container_var}bronze/product_demand/pdo_loading/"

# COMMAND ----------

# load landing paths 
ph_pdo_loaded_df = []
dates = generate_date(start_date , end_date,"%Y%m%d")
for day in dates:
    landing_path = f"{container_var}landing/product_demand/pdo_loading/{day}/ph_pdo_loading_report.csv"
    try:
        each_day_df = (spark.read.format('csv')
                            .option('header', True)
                            .option("inferSchema", "true")
                            .option("sep", ",")   
                            .load(landing_path))
        ph_pdo_loaded_df.append(each_day_df)
    except:
        print(f"The file {landing_path} does not exist")

# COMMAND ----------

pdo_loading_merged_df = ph_pdo_loaded_df[0]
if (len(ph_pdo_loaded_df) >1):
    for x in range(len(ph_pdo_loaded_df)):
        if(x >0):
            pdo_loading_merged_df = pdo_loading_merged_df.unionAll(ph_pdo_loaded_df[x])

# COMMAND ----------

pdo_loading_merged_df = pdo_loading_merged_df.select([col(cols).alias(cols.replace(' ','_').upper()) for cols in pdo_loading_merged_df.columns])

# COMMAND ----------

#Add water mark columns 
clean_pdo_loading_merged_df = (pdo_loading_merged_df
                      .withColumn("LOADING_DATE",date_format("LOADING_DATE","yyyyMMdd"))
                       .withColumn('RUN_DATE_ID', lit(run_date_int).cast('int'))
                       .withColumn("INSERT_LOAD_TIME_ID", lit(run_id).cast('int'))
                       .withColumn("INSERT_LOAD_TIME", lit(current_time).cast('timestamp')))

# COMMAND ----------

unique_dates = clean_pdo_loading_merged_df.select("LOADING_DATE").distinct().rdd.flatMap(lambda x: x).collect()

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
print(clean_pdo_loading_merged_df.count())

# COMMAND ----------

"""WRITTING INTO PDO LOADING"""
(clean_pdo_loading_merged_df.write.format("delta")
 .partitionBy("LOADING_DATE")
 .option("replaceWhere",  f"{partition_predicate}")
.mode("overwrite").save(bronze_path))

clean_pdo_loading_merged_df.unpersist(blocking=True)
