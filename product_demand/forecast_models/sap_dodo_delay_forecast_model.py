# Databricks notebook source
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from pyspark.sql.functions import lit, to_date,date_format,count,avg,max, regexp_replace, col
from datetime import datetime, timedelta
from prophet import Prophet

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

gold_path = f"{container_var}gold/product_demand/sap_dodo_feature_store/"
model_path = f"{container_var}gold/product_demand/sap_dodo_delay_model/"

# COMMAND ----------

# DBTITLE 1,GET ALL DATA WHERE DELIVERY IN NOT NULL
"""READING SILVER SAP AND DODO DATA """                       
sap_dodo_delay_df = (spark.read.format('delta')
    .load(gold_path).where(f"RUN_DATE_ID = {run_date_int} and isNotNull(DELIVERY_DAYS)")
    .withColumn("DELIVERY_STATION",regexp_replace("DELIVERY_STATION","\\'",""))
    )


# COMMAND ----------

stations = sap_dodo_delay_df.select('DELIVERY_STATION').where(f"isNotNull(DELIVERY_DATE)").distinct().toPandas()

# COMMAND ----------

# DBTITLE 1,DELAYS FORCASTING
loaded_stations_df = []
for station in range(stations.shape[0]):
    try:
        delays_df = sap_dodo_delay_df.select('DELIVERY_DATE','DELIVERY_DAYS').where(f"DELIVERY_STATION = '{stations['DELIVERY_STATION'].iloc[station]}' AND isNotNull(DELIVERY_DATE)")
        delays_df = delays_df.withColumnRenamed('DELIVERY_DATE','ds').withColumnRenamed('DELIVERY_DAYS','y')
        my_model = Prophet(interval_width=0.95)
        # We convert to pandas because prophet only works with pandas dataframe
        pandas_delays_df = delays_df.toPandas()
        my_model.fit(pandas_delays_df)
        future_dates = my_model.make_future_dataframe(periods=7, freq='D',include_history=True)
        forecast = my_model.predict(future_dates)
        final_forecast = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].copy()
        final_forecast['delivery_station'] = stations['DELIVERY_STATION'].iloc[station]
        final_forecast_spark = spark.createDataFrame(final_forecast)
        final_forecast_spark = final_forecast_spark.join(delays_df,['ds'])
        loaded_stations_df.append(final_forecast_spark)
    except Exception as e:
        print(f"An error occured: {e}")

# COMMAND ----------

merged_forecasted_delays_df = loaded_stations_df[0]
for x in range(1,len(loaded_stations_df)):
    merged_forecasted_delays_df = merged_forecasted_delays_df.unionAll(loaded_stations_df[x])

# COMMAND ----------

# DBTITLE 1,Prophet columns explained
# MAGIC %md
# MAGIC 1. ds: the datestamp of the forecasted value
# MAGIC 2. yhat: the forecasted value of our metric (in Statistics, yhat is a notation traditionally used to represent the predicted values of a value y)
# MAGIC 3. yhat_lower: the lower bound of our forecasts
# MAGIC 4. yhat_upper: the upper bound of our forecasts

# COMMAND ----------

merged_forecasted_delays_df = merged_forecasted_delays_df.withColumnRenamed('ds','FORECASTED_DATE').withColumnRenamed('yhat','FORCASTED_VALUE').withColumnRenamed('yhat_lower','LOWER_FORCASTED_VALUE').withColumnRenamed('yhat_upper','UPPER_FORCASTED_VALUE').withColumnRenamed('delivery_station','DELIVERY_STATION').withColumnRenamed('y','ACTUAL_VALUE')

# COMMAND ----------

merged_forecasted_delays_df = merged_forecasted_delays_df.withColumn("FORECASTED_DATE",date_format("FORECASTED_DATE","yyyyMMdd"))

# COMMAND ----------

#Add water mark columns 
merged_forecasted_delays_df = (merged_forecasted_delays_df
                               .withColumn('RUN_DATE_ID', lit(run_date_int).cast('int'))
                               .withColumn("INSERT_LOAD_TIME_ID", lit(run_id).cast('int'))
                               .withColumn("INSERT_LOAD_TIME", lit(current_time).cast('timestamp')))

# COMMAND ----------

merged_forecasted_delays_df = merged_forecasted_delays_df.withColumnRenamed('ds','FORECASTED_DATE').withColumnRenamed('yhat','FORCASTED_DELAY').withColumnRenamed('yhat_lower','LOWER_FORCASTED_DELAY').withColumnRenamed('yhat_upper','UPPER_FORCASTED_DELAY').withColumnRenamed('delivery_station','DELIVERY_STATION')

# COMMAND ----------

# Create an array of unique date
unique_dates = merged_forecasted_delays_df.select("FORECASTED_DATE").distinct().rdd.flatMap(lambda x: x).collect()

dates_processed = [] 
for dates_ in unique_dates : 
    dates_processed.append(dates_)

# COMMAND ----------

#Create a comma separated list
dates_processed_str = ",".join(str(i) for i in dates_processed)
partition_predicate = f"FORECASTED_DATE IN ({dates_processed_str}) "
#this is to force caching process
print(merged_forecasted_delays_df.count())


# COMMAND ----------

"""WRITTING INTO GOLD"""
(merged_forecasted_delays_df.write.format("delta")
 .partitionBy("FORECASTED_DATE")
 #.option("replaceWhere",  f"{partition_predicate}")
.mode("overwrite").save(model_path))

merged_forecasted_delays_df.unpersist(blocking=True)
