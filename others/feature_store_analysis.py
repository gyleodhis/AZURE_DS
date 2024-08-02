# Databricks notebook source
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from pyspark.sql.functions import  col, lower , concat_ws ,regexp_replace ,trim

# COMMAND ----------

credential = DefaultAzureCredential()
secret_client = SecretClient(vault_url="https://azkeyvault2f2ph6.vault.azure.net/", credential=credential)

# COMMAND ----------

storage_account_name = "azwksdatalake2f2ph6.blob.core.windows.net"
container_name = "azsynapsewks2f2ph6"
container_var = f"wasbs://{container_name}@{storage_account_name}/"


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

gold_path = f"{container_var}gold/product_demand/sap_dodo_feature_store/"

# COMMAND ----------

feature_store_df = (spark.read.format('delta')
       .load(gold_path))


# COMMAND ----------

feature_store_df.display()
