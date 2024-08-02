# Databricks notebook source
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from pyspark.sql.functions import  col, upper ,trim ,lit, to_date ,when, split, length, concat
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
bronze_path = f"{container_var}bronze/product_demand/codo_stations_stock_volume/"
silver_path = f"{container_var}silver/product_demand/codo_stations_stock_volume/"


# COMMAND ----------

"""READING IN STATION STOCK VOLUME"""                       
codo_station_stock_volume_df = (spark.read.format('delta')
    .load(bronze_path).where(f"RUN_DATE_ID = {run_date_int}"))

# COMMAND ----------

codo_station_stock_volume_df = (codo_station_stock_volume_df
                                .withColumn("DATE_CREATED",
                                            concat(to_date("DATE_CREATED","dd/MM/yyyy H:mm"),
                                                        lit(" "),
                                                        when(length(split("DATE_CREATED"," ")[1])==4,
                                                                concat(lit(0),split("DATE_CREATED"," ")[1]))
                                                       .otherwise(split("DATE_CREATED"," ")[1]))))

# COMMAND ----------

codo_station_stock_volume_df = codo_station_stock_volume_df.select("RECORD_ID",
                                    "STATION",
                                    "TANK_DESCRIPTION",
                                    "PRODUCT",
                                    col("DIP_READING").cast("int").alias("DIP_READING"),
                                    "STATUS",
                                    "SALES_DATE",
                                    "DATE_CREATED",
                                    "FILE_NAME"
                                    )                                  

# COMMAND ----------

#Add watermark columns 
clean_codo_station_stock_vol_df = (codo_station_stock_volume_df                      
                       .withColumn('RUN_DATE_ID', lit(run_date_int).cast('int'))
                       .withColumn("INSERT_LOAD_TIME_ID", lit(run_id).cast('int'))
                       .withColumn("INSERT_LOAD_TIME", lit(current_time).cast('timestamp')))

# COMMAND ----------

unique_dates = clean_codo_station_stock_vol_df.select("SALES_DATE").distinct().rdd.flatMap(lambda x: x).collect()

dates_processed = [] 
for dates_ in unique_dates : 
    dates_processed.append(dates_)

# COMMAND ----------

#Create a comma separated list
dates_processed_str = ",".join(str(i) for i in dates_processed)

# COMMAND ----------

partition_predicate = f"SALES_DATE IN ({dates_processed_str}) "

# COMMAND ----------

"""WRITTING IN STATION STOCK VOLUME"""
(clean_codo_station_stock_vol_df.write.format("delta")
 .partitionBy("SALES_DATE")
 .option("replaceWhere",  f"{partition_predicate}")
.mode("overwrite").save(silver_path))
