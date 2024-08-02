# Databricks notebook source
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from pyspark.sql.functions import lit, to_date,date_format,sum,max,col,avg,round,date_add,concat,trim
from datetime import datetime, timedelta
from prophet import Prophet

import time

# COMMAND ----------

credential = DefaultAzureCredential()
secret_client = SecretClient(vault_url="https://azkeyvault2f2ph6.vault.azure.net/", credential=credential)

storage_account_name = "azwksdatalake2f2ph6.blob.core.windows.net"
container_name = "azsynapsewks2f2ph6"
synapse_temp_dir = f"wasbs://{container_name}@{storage_account_name}/synapse/temp/"
config_path = f"wasbs://{container_name}@{storage_account_name}/config/"

# COMMAND ----------


storage_account_key = secret_client.get_secret("storage-account-key").value
storage_sas_token = secret_client.get_secret("sas-token").value
synapse_connection_string = secret_client.get_secret("synapse-con-str").value
table_name = 'CODO_FORECAST'
new_table_name = 'NEW_CODO_FORECAST'

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

run_id = int(time.mktime(current_time.timetuple()))
run_date_int = datetime.fromtimestamp(run_id).strftime("%Y%m%d")

print("Run Date: " + str(run_date_int))
print("run id: " + str(run_date_int))

# COMMAND ----------

container_var = f"wasbs://{container_name}@{storage_account_name}/"

sales_path = f"{container_var}gold/product_demand/codo_sales_model/"
quantity_path = f"{container_var}gold/product_demand/codo_raw_model/"
next_delivery_path = f"{container_var}gold/product_demand/codo_next_delivery_model/"

# COMMAND ----------

"""READING CODO FEATURE STORE """                       
codo_quantity_df = (spark.read.format('delta')
    .load(quantity_path)#.where(f"RUN_DATE_ID = {run_date_int} AND {end_date}")
    )
codo_quantity_df = codo_quantity_df.withColumnRenamed('ACTUAL_VALUE','ACTUAL_QUANTITY') \
    .withColumnRenamed('FORCASTED_DAILY_SALES_VOLUME','FORECASTED_QUANTITY')
codo_quantity_df = codo_quantity_df[['FORECASTED_SALES_DATE','FORECASTED_QUANTITY','ACTUAL_QUANTITY',trim('STATION').alias('STATION')]]
codo_next_delivery_df = (spark.read.format('delta')
    .load(next_delivery_path)#.where(f"RUN_DATE_ID = {run_date_int}")
    )
codo_next_delivery_df = codo_next_delivery_df[['FORECASTED_SALES_DATE','FORCASTED_NEXT_DELIVERY_DAYS','ACTUAL_VALUE','STATION']] \
    .withColumnRenamed('ACTUAL_VALUE','ACTUAL_NEXT_DELIVERY_DAYS')

codo_sales_df = (spark.read.format('delta')
    .load(sales_path)#.where(f"RUN_DATE_ID = {run_date_int} AND isNotNull('ACTUAL_VOLUME_SOLD')")
    )
codo_sales_df = codo_sales_df[['FORECASTED_SALES_DATE','STATION','FORCASTED_DAILY_SALES_VOLUME','ACTUAL_VOLUME_SOLD']]

# COMMAND ----------


codo_forecast =  codo_next_delivery_df.join(codo_sales_df,['STATION','FORECASTED_SALES_DATE'])
codo_final_forecast =  codo_forecast.join(codo_quantity_df,['STATION','FORECASTED_SALES_DATE'])

# COMMAND ----------

codo_final_forecast =codo_final_forecast.na.drop('any')

# COMMAND ----------


# I do not need this for now but I might need it later
codo_final_forecast = codo_final_forecast.withColumn('FORCASTED_NEXT_DELIVERY_DAYS',round('FORCASTED_NEXT_DELIVERY_DAYS',0)) \
    .withColumn('FORCASTED_DAILY_SALES_VOLUME',round('FORCASTED_DAILY_SALES_VOLUME',0))

# COMMAND ----------

codo_final_forecast = codo_final_forecast.withColumn('NEXT_OPTIMAL_ORDER_DATE', \
    date_format(date_add(codo_final_forecast['FORECASTED_SALES_DATE'], \
    round((codo_final_forecast['ACTUAL_QUANTITY'] * 0.5)/codo_final_forecast['FORCASTED_DAILY_SALES_VOLUME'],0) \
        .astype('int')),'yyyy-MM-dd'))

# COMMAND ----------

codo_final_forecast = codo_final_forecast.withColumn('NEXT_OPTIMAL_ORDER_DATE_REASON', \
    concat(lit("Your order likely to take: "), col("FORCASTED_NEXT_DELIVERY_DAYS"), lit(" days")))

# COMMAND ----------


codo_final_forecast = (codo_final_forecast
                               .withColumn('RUN_DATE_ID', lit(run_date_int).cast('int'))
                               .withColumn("INSERT_LOAD_TIME_ID", lit(run_id).cast('int'))
                               .withColumn("INSERT_LOAD_TIME", lit(current_time).cast('timestamp')))

# COMMAND ----------

(codo_final_forecast.write
                        .format('com.databricks.spark.sqldw')
                        .option('url', synapse_connection_string)
                        .option('forwardSparkAzureStorageCredentials', 'true')
                        .option('dbTable', f"{new_table_name}")
                        .option('tempDir', synapse_temp_dir)
                        .mode('overwrite')
                        .save())
