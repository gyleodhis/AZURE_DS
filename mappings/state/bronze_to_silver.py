# Databricks notebook source
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from pyspark.sql.functions import  col, upper ,lit, to_date , split
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
bronze_path = f"{container_var}bronze/mapping/state/"
silver_path = f"{container_var}silver/mapping/state/"


# COMMAND ----------

"""READING NIGERIAN STATE """                       
state_df = (spark.read.format('delta')
    .load(bronze_path).where(f"RUN_DATE_ID = {run_date_int}"))

# COMMAND ----------

state_df = (state_df.withColumn("STATE_ID",col("SNO"))
                                .withColumn("STATE_NAME",upper(split(col("STATE")," ")[0]))
                                .withColumn("LOCAL_GOVERNMENT_AREA",upper(col("LOCAL_GOVERNMENT_AREA")))
                                .drop("STATE","SNO"))

# COMMAND ----------

final_state_df = state_df.select("STATE_ID",
                "STATE_NAME",
                "LOCAL_GOVERNMENT_AREA")

# COMMAND ----------

#Add watermark columns 
final_state_df = (final_state_df                      
                       .withColumn('RUN_DATE_ID', lit(run_date_int).cast('int'))
                       .withColumn("INSERT_LOAD_TIME_ID", lit(run_id).cast('int'))
                       .withColumn("INSERT_LOAD_TIME", lit(current_time).cast('timestamp')))

# COMMAND ----------

"""WRITTING STATES"""
(final_state_df.write.format("delta")
.mode("overwrite").save(silver_path))
