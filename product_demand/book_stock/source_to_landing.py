# Databricks notebook source
import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from datetime import datetime, timedelta
from lib.blob_utils import read_from_blob,upload_csv_to_blob
from lib.utils import date_convertor
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

dbutils.widgets.text("file_name","None")
file_name = dbutils.widgets.get("file_name")

print(f"Processing: {file_name}")

# COMMAND ----------

if file_name == 'None':
    dbutils.notebook.exit("No files to load")

# COMMAND ----------

"""DEFINE PATHS"""
container_var = f"wasbs://{container_name}@{storage_account_name}/"
landing_path = f"landing/product_demand/book_stock_of_products/"

# COMMAND ----------

source_path = f"source/product_demand/book_stock_of_products/{file_name}.xlsx"

# COMMAND ----------

def write_to_blob(xlsx_path: str):
    # file_name =[]
    tabs = pd.ExcelFile(read_from_blob(xlsx_path)).sheet_names
    del tabs[2::3]
    for sheet_name in tabs:
        try:
            df_bookStock = pd.read_excel(read_from_blob(xlsx_path), sheet_name=sheet_name,
                                     dtype=str,skiprows=2,usecols=lambda x: 'Unnamed' not in x)
            k = sheet_name.split('_')
            if len(k) > 1:
                file_name = f"{k[0]}_{k[1]}_{date_convertor(k[2],'%d%m%y','%Y%m%d')}"
                file_path = f"{landing_path}{date_convertor(k[2],'%d%m%y','%Y%m%d')}"
            else:
                file_name = f"{date_convertor(k[0],'%d%m%y','%Y%m%d')}"
                file_path = f"{landing_path}{date_convertor(k[0],'%d%m%y','%Y%m%d')}"
            upload_csv_to_blob(df_bookStock,file_name,file_path)
        except Exception as e:
            print(f"Could not load sheet: {sheet_name} because of {e}")

# COMMAND ----------

write_to_blob(source_path)
