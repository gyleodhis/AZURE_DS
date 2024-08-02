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
bronze_path = f"{container_var}bronze/mapping/state/"


# COMMAND ----------

# load landing paths 
landing_path = f"{container_var}mapping/states/list_of_local_governments.csv"
state_df = (spark.read.format('csv')
                            .option('header', True)
                            .option("inferSchema", "true")
                            .option("sep", ",")   
                            .load(landing_path)
                            .withColumn("FILE_NAME", input_file_name()))
      


# COMMAND ----------

state_df = state_df.select([col(cols).alias(cols.replace(' ','_').upper()) for cols in state_df.columns]).drop("_c3")

# COMMAND ----------

#Add water mark columns 
final_state_df = (state_df
                       .withColumn('RUN_DATE_ID', lit(run_date_int).cast('int'))
                       .withColumn("INSERT_LOAD_TIME_ID", lit(run_id).cast('int'))
                       .withColumn("INSERT_LOAD_TIME", lit(current_time).cast('timestamp')))

# COMMAND ----------

"""WRITTING STATES DATA"""
(final_state_df.write.format("delta") 
   .mode("overwrite").save(bronze_path))

