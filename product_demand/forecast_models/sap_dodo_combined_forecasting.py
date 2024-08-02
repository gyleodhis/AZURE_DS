# Databricks notebook source
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from pyspark.sql.functions import lit,round
from datetime import datetime

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
synapse_connection_string = secret_client.get_secret("synapse-con-str").value
table_name = 'SAP_DODO_FORECAST'

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

run_id = int(time.mktime(current_time.timetuple()))
run_date_int = datetime.fromtimestamp(run_id).strftime("%Y%m%d")

print("Run Date: " + str(run_date_int))
print("run id: " + str(run_date_int))

# COMMAND ----------

# define paths 
container_var = f"wasbs://{container_name}@{storage_account_name}/"

oders_path = f"{container_var}gold/product_demand/sap_dodo_orders_model/"
delays_path = f"{container_var}gold/product_demand/sap_dodo_delay_model/"

# COMMAND ----------

"""READING BOTH MODELS """                       
orders_df = (spark.read.format('delta')
    .load(oders_path)#.where(f"RUN_DATE_ID = {run_date_int}")
    )
orders_df = orders_df[['FORECASTED_DATE','FORCASTED_ORDER','DELIVERY_STATION','ACTUAL_VALUE']] \
    .withColumnRenamed('ACTUAL_VALUE','ACTUAL_ORDER')
delays_df = (spark.read.format('delta')
    .load(delays_path)#.where(f"RUN_DATE_ID = {run_date_int}")
    )
delays_df = delays_df[['FORECASTED_DATE','FORCASTED_VALUE','DELIVERY_STATION','ACTUAL_VALUE']] \
    .withColumnRenamed('ACTUAL_VALUE','ACTUAL_DELIVERY_DAYS') \
    .withColumnRenamed('FORCASTED_VALUE','FORCASTED_DELIVERY_DAYS')

# COMMAND ----------

orders_df = (orders_df
                               .withColumn('RUN_DATE_ID', lit(run_date_int).cast('int'))
                               .withColumn("INSERT_LOAD_TIME_ID", lit(run_id).cast('int'))
                               .withColumn("INSERT_LOAD_TIME", lit(current_time).cast('timestamp')))

# COMMAND ----------

delays_df = delays_df.withColumn('FORCASTED_DELIVERY_DAYS',round(delays_df["FORCASTED_DELIVERY_DAYS"], 0))

# COMMAND ----------

sap_dodo_forecast_final_df = delays_df.join(orders_df,['FORECASTED_DATE','DELIVERY_STATION'])

# COMMAND ----------

(sap_dodo_forecast_final_df.write
                        .format('com.databricks.spark.sqldw')
                        .option('url', synapse_connection_string)
                        .option('forwardSparkAzureStorageCredentials', 'true')
                        .option('dbTable', f"{table_name}")
                        .option('tempDir', synapse_temp_dir)
                        .mode('overwrite')
                        .save())

