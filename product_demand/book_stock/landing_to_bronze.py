# Databricks notebook source
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from pyspark.sql.functions import  col, upper,lit,date_format,input_file_name
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

# load landing paths 
dates = generate_date(start_date , end_date,"%Y%m%d")
for day in dates:
    try:
        dbutils.notebook.run("./load_book_stock_data", 60, {"process_date": day,"file_name_prefix":"NRL_ABUJA"})
        dbutils.notebook.run("./load÷_book_stock_data", 60, {"process_date": day,"file_name_prefix":"NRL_LAGOS"})
    except:
        print(f"The file does not exist")
