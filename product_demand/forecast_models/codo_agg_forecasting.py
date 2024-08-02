# Databricks notebook source
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from pyspark.sql.functions import lit, to_date,date_format,sum,max,col
from datetime import datetime, timedelta
from prophet import Prophet

import time

# COMMAND ----------

credential = DefaultAzureCredential()
secret_client = SecretClient(vault_url="https://azkeyvault2f2ph6.vault.azure.net/", credential=credential)

storage_account_name = "azwksdatalake2f2ph6.blob.core.windows.net"
container_name = "azsynapsewks2f2ph6"
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

run_id = int(time.mktime(current_time.timetuple()))
run_date_int = datetime.fromtimestamp(run_id).strftime("%Y%m%d")

print("Run Date: " + str(run_date_int))
print("run id: " + str(run_date_int))

# COMMAND ----------

container_var = f"wasbs://{container_name}@{storage_account_name}/"

gold_path = f"{container_var}gold/product_demand/codo_agg_feature_store/"
model_path = f"{container_var}gold/product_demand/codo_model/"

# COMMAND ----------

"""READING CODO FEATURE STORE """                       
codo_df = (spark.read.format('delta')
    .load(gold_path).where(f"RUN_DATE_ID = {run_date_int}")
    )
codo_df.display()

# COMMAND ----------

new_codo_df = codo_df.groupBy("STATION","MATERIAL","CURRENT_DELIVERY_DATE","SALES_DATE","TOTAL_STATION_CAPACITY","QUANTITY") \
    .agg(sum("TOTAL_DAILY_VOLUME_PRODUCT_SOLD").alias("TOTAL_VOLUME_SOLD"),max("TOTAL_VOLUME_SOLD_POST_CURRENT_DELIVERY_DATE").alias("TOTAL_VOLUME_SOLD_POST_CURRENT_DELIVERY"))
new_codo_df.orderBy("STATION","CURRENT_DELIVERY_DATE","SALES_DATE").display(truncate=False)

# COMMAND ----------

new_codo_df1 = codo_df.groupBy("STATION","MATERIAL","DELIVERY_DATE","SALES_DATE","TOTAL_STATION_CAPACITY","QUANTITY") \
    .agg(max("TOTAL_DAILY_VOLUME_PRODUCT_SOLD").alias("TOTAL_VOLUME_SOLD"),max("TOTAL_STATION_CAPACITY").alias("TOTAL_STATION_CAPACITY"))
new_codo_df1.orderBy("STATION",col("DELIVERY_DATE").desc(),col("SALES_DATE").asc()).display(truncate=False)
