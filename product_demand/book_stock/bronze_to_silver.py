# Databricks notebook source
# DBTITLE 1,Import Modules
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from pyspark.sql.functions import  col,lit
from datetime import datetime
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

run_id = int(time.mktime(current_time.timetuple()))
run_date_int = datetime.fromtimestamp(run_id).strftime("%Y%m%d")

print("Run Date: " + str(run_date_int))
print("run id: " + str(run_date_int))

# COMMAND ----------

# DBTITLE 1,Blob file paths
# define paths 
container_var = f"wasbs://{container_name}@{storage_account_name}/"
stations = ['NRL_ABUJA','NRL_LAGOS']
loaded_stations_df = []
for station in stations:
    try:
        bronze_path = f"{container_var}bronze/product_demand/book_stock_of_products/{station}/"
        silver_path = f"{container_var}silver/product_demand/book_stock_of_products/"
        """READING IN STATION STOCK VOLUME"""                      
        book_stock_df = (spark.read.format('delta')
                         .load(bronze_path).where(f"RUN_DATE_ID = {run_date_int}"))
        """Adding station column which will indicate the source station of the file."""
        book_stock_df = book_stock_df.withColumn('STATION',lit(station))
        loaded_stations_df.append(book_stock_df)
    except Exception as e:
        print(f"An error occured: {e}")

# COMMAND ----------

# DBTITLE 1,Merging the two book stocks into one dataframe
merged_book_stock_df = loaded_stations_df[0]
for x in range(1,len(loaded_stations_df)):
    merged_book_stock_df = merged_book_stock_df.unionAll(loaded_stations_df[x])

# COMMAND ----------

# DBTITLE 1,Selecting relevant columns
book_stock_df = book_stock_df.select(col("S/N").alias("SERIAL_NO"),
                                    "DEPOT_NAME",
                                    "OPENING_PMS_BALANCE",
                                    "PMS_LOAD_OUT",
                                    "CLOSING_PMS_BALANCE_LITRES",
                                    "OPENING_AGO_BALANCE",
                                    "AGO_LOAD_OUT",
                                    "CLOSING_AGO_BALANCE",
                                    "OPENING_DPK_BALANCE",
                                    "DPK_LOAD_OUT",
                                    "CLOSING_DPK_BALANCE_LITRES",
                                    "BOOK_STOCK_DATE",
                                    "FILE_NAME",
                                    "STATION"
                                    )

# COMMAND ----------

# DBTITLE 1,Adding Watermark Columns
#Add watermark columns 
book_stock_df = (book_stock_df                      
                       .withColumn('RUN_DATE_ID', lit(run_date_int).cast('int'))
                       .withColumn("INSERT_LOAD_TIME_ID", lit(run_id).cast('int'))
                       .withColumn("INSERT_LOAD_TIME", lit(current_time).cast('timestamp')))

# COMMAND ----------

# DBTITLE 1,Creating a partition predicated from unique dates
unique_dates = book_stock_df.select("BOOK_STOCK_DATE").distinct().rdd.flatMap(lambda x: x).collect()

dates_processed = [] 
for dates_ in unique_dates : 
    dates_processed.append(dates_)

#Create a comma separated list
dates_processed_str = ",".join(str(i) for i in dates_processed)

partition_predicate = f"BOOK_STOCK_DATE IN ({dates_processed_str}) "

# COMMAND ----------

# DBTITLE 1,Writing to Silver
"""WRITTING IN STATION STOCK VOLUME"""
(book_stock_df.write.format("delta")
 .partitionBy("BOOK_STOCK_DATE")
 .option("replaceWhere",  f"{partition_predicate}")
.mode("overwrite").save(silver_path))
