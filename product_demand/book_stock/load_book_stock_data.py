# Databricks notebook source
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from pyspark.sql.functions import  col, upper,lit,date_format,input_file_name,trim
from datetime import datetime, timedelta
from lib.utils import generate_date,date_convertor

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

dbutils.widgets.text("process_date",(current_time-timedelta(1)).strftime("%Y%m%d"))

dbutils.widgets.text("file_name_prefix","None")

process_date = dbutils.widgets.get("process_date")
file_name_prefix = dbutils.widgets.get("file_name_prefix")

run_id = int(time.mktime(current_time.timetuple()))
run_date_int = datetime.fromtimestamp(run_id).strftime("%Y%m%d")


print("Process Date: " + str(process_date))
print("run id: " + str(run_date_int))

# COMMAND ----------

"""DEFINE PATHS"""
container_var = f"wasbs://{container_name}@{storage_account_name}/"
bronze_path = f"{container_var}bronze/product_demand/book_stock_of_products/{file_name_prefix}"

# COMMAND ----------

if file_name_prefix == 'None':
    dbutils.notebook.exit("No files to load")

# COMMAND ----------

# load landing paths 
landing_path = f"{container_var}landing/product_demand/book_stock_of_products/{process_date}/{file_name_prefix}_{process_date}.csv"
try:
    book_stock_df = (spark.read.format('csv')
                            .option('header', True)
                            .option("inferSchema", "true")
                            .option("sep", ",")   
                            .load(landing_path)
                            .withColumn("FILE_NAME", input_file_name()))        
except:
    print(f"The file {landing_path} does not exist")

# COMMAND ----------

book_stock_df = book_stock_df.select([col(cols).alias(cols.replace(' ','_').upper()) for cols in book_stock_df.columns])

# COMMAND ----------


mapping = ["OPENING_PMS_BALANCE","PMS_LOAD_OUT","CLOSING_PMS_BALANCE_LITRES","OPENING_AGO_BALANCE",
           "AGO_LOAD_OUT","CLOSING_AGO_BALANCE","OPENING_DPK_BALANCE","DPK_LOAD_OUT","CLOSING_DPK_BALANCE_LITRES"]
df_columns = book_stock_df.columns


# COMMAND ----------



for i in range(len(mapping)):
    index = [idx for idx, s in enumerate(df_columns) if mapping[i] in s][0]
    if(mapping[i])=="OPENING_PMS_BALANCE":
        #extract date from column name and create a new column      
        book_stock_date = date_convertor(df_columns[index].split("_")[3],"%d-%B-%y","%Y%m%d")
        book_stock_df = book_stock_df.withColumn("BOOK_STOCK_DATE",lit(book_stock_date)) 
    #Rename columns by removing the date in column names 
    book_stock_df = book_stock_df.withColumnRenamed(df_columns[index],mapping[i])


# COMMAND ----------

#Add water mark columns 
final_book_stock_df = (book_stock_df                      
                       .withColumn('RUN_DATE_ID', lit(run_date_int).cast('int'))
                       .withColumn("INSERT_LOAD_TIME_ID", lit(run_id).cast('int'))
                       .withColumn("INSERT_LOAD_TIME", lit(current_time).cast('timestamp')))

# COMMAND ----------

partition_predicate = f"BOOK_STOCK_DATE IN ({process_date}) "

# COMMAND ----------

#this is to force caching process
print(final_book_stock_df.count())

# COMMAND ----------

"""WRITTING INTO BOOK STOCK"""

(final_book_stock_df.write.format("delta")
 .partitionBy("BOOK_STOCK_DATE")
 .option("replaceWhere",  f"{partition_predicate}")
.mode("overwrite").save(bronze_path))

final_book_stock_df.unpersist(blocking=True)
