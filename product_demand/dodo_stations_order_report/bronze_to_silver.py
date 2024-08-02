# Databricks notebook source
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from pyspark.sql.functions import  col, upper,lit,when,regexp_replace
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
bronze_path = f"{container_var}bronze/product_demand/dodo_stations_order_report/"
silver_path = f"{container_var}silver/product_demand/dodo_stations_order_report/"

# COMMAND ----------

"""READING IN DODO STATIONS ORDER REPORT"""                       
dodo_stations_order_report_df = (spark.read.format('delta')
    .load(bronze_path).where(f"RUN_DATE_ID = {run_date_int}"))


# COMMAND ----------

# cleaning up preceeding single quotes and zeros in SAP_QUOTATION_NUMBER eg '0020291914
dodo_stations_order_report_df = (dodo_stations_order_report_df
                                .withColumn("SAP_QUOTATION_NUMBER",regexp_replace(col("SAP_QUOTATION_NUMBER"),"'00",""))
                                .withColumn("SHIPMENT_DOC_NUMBER",regexp_replace(col("SHIPMENT_DOC_NUMBER"),"'",""))
                                .withColumn("DELIVERY_DOC_NUMBER",regexp_replace(col("DELIVERY_DOC_NUMBER"),"'",""))
                                .withColumn("QUANTITY",regexp_replace(col("QUANTITY"),",","").cast("double"))  
                                .withColumn("QUANTITY_RECEIVED",regexp_replace(col("QUANTITY_RECEIVED"),",","").cast("double"))  
                                .withColumn("QUANTITY_SOLD",regexp_replace(col("QUANTITY_SOLD"),",","").cast("double"))
                                 )

# COMMAND ----------

dodo_stations_order_report_df = dodo_stations_order_report_df.select("CUSTOMER_NAME",
                    "SAP_QUOTATION_NUMBER",
                    "PRODUCT",
                    "QUANTITY",
                    "RRR_NUMBER",
                    "SAP_PAYMENT_NUMBER",
                    "AMOUNT_PAID",
                    "SAP_SALES_ORDER_NUMBER",
                    "QUANTITY_SOLD",
                    "DELIVERY_DOC_NUMBER",
                    "SHIPMENT_DOC_NUMBER",
                    "QUANTITY_RECEIVED",
                    "DELIVERY_TRUCK_PLATE_NUMBER",
                    "CLEARING_DOC_NUMBER",
                    "SALES_DATE",
                    "STATE",
                    "REGION",
                    "COMPLETED_ON",
                    "FILE_NAME",
                    "SALES_TIME"
                    )

# COMMAND ----------

#Add watermark columns 
dodo_stations_order_report_df = (dodo_stations_order_report_df                      
                       .withColumn('RUN_DATE_ID', lit(run_date_int).cast('int'))
                       .withColumn("INSERT_LOAD_TIME_ID", lit(run_id).cast('int'))
                       .withColumn("INSERT_LOAD_TIME", lit(current_time).cast('timestamp')))

# COMMAND ----------

unique_dates = dodo_stations_order_report_df.select("SALES_DATE").distinct().rdd.flatMap(lambda x: x).collect()

dates_processed = [] 
for dates_ in unique_dates : 
    dates_processed.append(dates_)

# COMMAND ----------

#Create a comma separated list
dates_processed_str = ",".join(str(i) for i in dates_processed)
# Adding partition predicate
partition_predicate = f"SALES_DATE IN ({dates_processed_str}) "

# COMMAND ----------

"""WRITTING DODO STATIONS ORDER"""
(dodo_stations_order_report_df.write.format("delta")
 .partitionBy("SALES_DATE")
 .option("replaceWhere",  f"{partition_predicate}")
.mode("overwrite").save(silver_path))

