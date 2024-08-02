# Databricks notebook source
# DBTITLE 1,Import Modules
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from pyspark.sql.functions import  col,trim ,lit, to_date ,when, split, length
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

# DBTITLE 1,Storage creds from key vault
#Retrieve storage credentals

storage_account_key = secret_client.get_secret("storage-account-key").value
storage_sas_token = secret_client.get_secret("sas-token").value


# COMMAND ----------

# DBTITLE 1,Spark configs
# Set up configs

spark.conf.set(
f"fs.azure.account.key.{storage_account_name}", f"{storage_account_key}"
)

spark.conf.set(
f"fs.azure.sas.{container_name}.{storage_account_name}",
f"{storage_sas_token}"
)

# COMMAND ----------

# DBTITLE 1,Date Filters
current_time = datetime.now()
dbutils.widgets.text("start_date",(current_time-timedelta(1)).strftime("%Y-%m-%d"))
dbutils.widgets.text("end_date",(current_time-timedelta(1)).strftime("%Y-%m-%d"))

start_date_str = dbutils.widgets.get("start_date")
end_date_str = dbutils.widgets.get("end_date")

run_id = int(time.mktime(current_time.timetuple()))
run_date_int = datetime.fromtimestamp(run_id).strftime("%Y%m%d")

start_date = datetime.strptime(start_date_str,"%Y-%m-%d").strftime("%Y%m%d")
end_date = datetime.strptime(end_date_str,"%Y-%m-%d").strftime("%Y%m%d")

print("Start date: " + str(start_date))
print("End date: " + str(end_date))
print("run id: " + str(run_date_int))

# COMMAND ----------

# DBTITLE 1,Blob file paths
# define paths 
container_var = f"wasbs://{container_name}@{storage_account_name}/"
bronze_path = f"{container_var}bronze/product_demand/pdo_loading/"
silver_path = f"{container_var}silver/product_demand/ph_pdo_loading/"


# COMMAND ----------

# DBTITLE 1,Loading from bronze where date is passed
"""READING IN STATION STOCK VOLUME"""                       
ph_pdo_loading_df = (spark.read.format('delta')
    .load(bronze_path).where(f"RUN_DATE_ID BETWEEN {start_date} AND {end_date}"))
ph_pdo_loading_df.display()

# COMMAND ----------

# DBTITLE 1,Selecting relevant columns
ph_pdo_loading_df = ph_pdo_loading_df.select("METER_TICKET_NUMBER",
                                    "DISPATCH_NUMBER",
                                    "PLAN_DATE",
                                    "DOCUMENT_REFERENCE",
                                    "CUSTOMER_NAME",
                                    "TRUCK_NUMBER",
                                    "DESTINATION",
                                    "PRODUCT_TYPE","LOADING_DEPOT",
                                    "LOADING_DATE",
                                    "LOCATION",
                                    "TD_ZONE",
                                    "MARKETER_REP",
                                    col("QUANTITY_TO_LOAD"),#.cast('float').alias("QUANTITY_TO_LOAD"),
                                    "LOADED_QUANTITY",
                                    "VARIANCE",
                                    "FILE_NAME"
                                    )
ph_pdo_loading_df.display()   

# COMMAND ----------

# DBTITLE 1,Adding Watermark Columns
#Add watermark columns 
ph_pdo_loading_df = (ph_pdo_loading_df                      
                       .withColumn('RUN_DATE_ID', lit(run_date_int).cast('int'))
                       .withColumn("INSERT_LOAD_TIME_ID", lit(run_id).cast('int'))
                       .withColumn("INSERT_LOAD_TIME", lit(current_time).cast('timestamp')))
ph_pdo_loading_df.display()

# COMMAND ----------

# DBTITLE 1,Creating a partition predicated from unique dates
unique_dates = ph_pdo_loading_df.select("LOADING_DATE").distinct().rdd.flatMap(lambda x: x).collect()

dates_processed = [] 
for dates_ in unique_dates : 
    dates_processed.append(dates_)

#Create a comma separated list
dates_processed_str = ",".join(str(i) for i in dates_processed)

partition_predicate = f"LOADING_DATE IN ({dates_processed_str}) "

# COMMAND ----------

# DBTITLE 1,Writing to Silver
"""WRITTING IN STATION STOCK VOLUME"""
(ph_pdo_loading_df.write.format("delta")
 .partitionBy("LOADING_DATE")
 .option("replaceWhere",  f"{partition_predicate}")
.mode("overwrite").save(silver_path))
