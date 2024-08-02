# Databricks notebook source
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from pyspark.sql.functions import  col,when, lit,regexp_replace
from datetime import datetime, timedelta

import time


# COMMAND ----------

#Set up credentials
credential = DefaultAzureCredential()
secret_client = SecretClient(vault_url="https://azkeyvault2f2ph6.vault.azure.net/", credential=credential)

storage_account_name = "azwksdatalake2f2ph6.blob.core.windows.net"
container_name = "azsynapsewks2f2ph6"
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

run_id = int(time.mktime(current_time.timetuple()))
run_date_int = datetime.fromtimestamp(run_id).strftime("%Y%m%d")

print("Run Date: " + str(run_date_int))
print("run id: " + str(run_date_int))

# COMMAND ----------

# define paths 
container_var = f"wasbs://{container_name}@{storage_account_name}/"
bronze_path = f"{container_var}bronze/product_demand/sap_loading/"
silver_path = f"{container_var}silver/product_demand/sap_loading/"

# COMMAND ----------

"""READING IN SAP LOADING """                       
sap_loading_df = (spark.read.format('delta')
    .load(bronze_path).where(f"RUN_DATE_ID = {run_date_int}"))

# COMMAND ----------

#-9999 value means null. Please filter out in your calculation.
sap_loading_df = (sap_loading_df
                           .withColumn("QUANTITY_ORDERED",regexp_replace(col("QUANTITY_ORDERED"),",","").cast("double"))
                           .withColumn("QUANTITY_DELIVERED",regexp_replace(col("QUANTITY_DELIVERED"),",","").cast("double"))
                           .withColumn("QUANTITY_DISPATCHED",regexp_replace(col("QUANTITY_DISPATCHED"),",","").cast("double"))  
                           .withColumn("QUANTITY_LOADED",regexp_replace(col("QUANTITY_LOADED"),",","").cast("double"))  
                                )

# COMMAND ----------

sap_loading_df = sap_loading_df.select("SALES_QUOTATION",
                                       "RRR_NUMBER",
                                       "SALES_DOCUMENT",
                                       "DISPATCH_NUMBER",
                                       "WAYBILL_NUMBER",
                                       "RESERVATION_NO",
                                       "TICKET_NUMBER",
                                       "LOADING_DEPOT_NAME",
                                       "PRODUCT_TYPE",
                                       "BATCH",
                                       "TRANSACTION_TYPE",
                                       "QUANTITY_LOADED",
                                       "TRANSACTION_UOM12",
                                       "LOADING_DATE",
                                       "MONTH",
                                       "REGISTERED_OWNER",
                                       "VEHICLE_NUMBER",
                                       "RECEIVING_DEPOT",
                                       "TRANSIT_TRUCK_STATUS",
                                       "QUANTITY_ORDERED",
                                       "QUANTITY_DISPATCHED",
                                       "DISPATCH_DATE",
                                       "SHIPMENT_STATUS",
                                       "DESTINATION",
                                       "DELIVERY_STATION",
                                       "DISTRIBUTION_CHANNEL",
                                       "DELIVERY_DATE",
                                       "QUANTITY_DELIVERED",
                                       "TRANSACTION_UOM28",
                                       "CREATED_BY",
                                       "CHANGED_BY",
                                       "FILE_NAME"
                                       )

# COMMAND ----------

#Add watermark columns 
sap_loading_df = (sap_loading_df                      
                       .withColumn('RUN_DATE_ID', lit(run_date_int).cast('int'))
                       .withColumn("INSERT_LOAD_TIME_ID", lit(run_id).cast('int'))
                       .withColumn("INSERT_LOAD_TIME", lit(current_time).cast('timestamp')))

# COMMAND ----------

unique_dates = sap_loading_df.select("LOADING_DATE").distinct().rdd.flatMap(lambda x: x).collect()

dates_processed = [] 
for dates_ in unique_dates : 
    dates_processed.append(dates_)

# COMMAND ----------

#Create a comma separated list
dates_processed_str = ",".join(str(i) for i in dates_processed)

# COMMAND ----------

partition_predicate = f"LOADING_DATE IN ({dates_processed_str}) "

# COMMAND ----------

"""WRITTING IN SAP LOADING """
(sap_loading_df.write.format("delta")
 .partitionBy("LOADING_DATE")
 .option("replaceWhere",  f"{partition_predicate}")
.mode("overwrite").save(silver_path))
