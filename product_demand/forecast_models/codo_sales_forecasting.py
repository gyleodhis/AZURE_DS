# Databricks notebook source
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from pyspark.sql.functions import lit, to_date,date_format,sum,max,col,avg,round
from datetime import datetime, timedelta
from prophet import Prophet

import time

# COMMAND ----------

credential = DefaultAzureCredential()
secret_client = SecretClient(vault_url="https://azkeyvault2f2ph6.vault.azure.net/", credential=credential)

storage_account_name = "azwksdatalake2f2ph6.blob.core.windows.net"
container_name = "azsynapsewks2f2ph6"
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

run_id = int(time.mktime(current_time.timetuple()))
run_date_int = datetime.fromtimestamp(run_id).strftime("%Y%m%d")

print("Run Date: " + str(run_date_int))
print("run id: " + str(run_date_int))

# COMMAND ----------

container_var = f"wasbs://{container_name}@{storage_account_name}/"

gold_path = f"{container_var}gold/product_demand/codo_raw_features/"
model_path = f"{container_var}gold/product_demand/codo_sales_model/"

# COMMAND ----------

"""READING CODO FEATURE STORE """                       
codo_df = (spark.read.format('delta')
    .load(gold_path).where(f"RUN_DATE_ID = {run_date_int} AND isNotNull(SALES_DATE)")
    )
# Converting sales_date from number to date format and type
codo_df= codo_df.withColumn("SALES_DATE", to_date(col("SALES_DATE").cast("string"),'yyyyMMdd'))

# COMMAND ----------

codo_raw_df = codo_df.groupBy("STATION","PRODUCT","SALES_DATE","QUANTITY","NEXT_DELIVERY_DAYS") \
    .agg(sum("VOLUME").alias("TOTAL_VOLUME_SOLD"),round(avg("VOLUME"),2).alias("AVG_VOLUME_SOLD_PER_DAY"))
codo_raw_df = codo_raw_df.withColumnRenamed('SALES_DATE','ds').withColumnRenamed('TOTAL_VOLUME_SOLD','y')

# COMMAND ----------


stations = codo_raw_df.select('STATION').where(f"isNotNull(SALES_DATE)").distinct().toPandas()

# COMMAND ----------

loaded_stations_df = []
for station in range(stations.shape[0]):
    try:
        sales_df = codo_raw_df.select('ds','y').where(f"STATION = '{stations['STATION'].iloc[station]}'")
        my_model = Prophet(interval_width=0.95)
        # We convert to pandas because prophet only works with pandas dataframe
        pandas_sales_df = sales_df.toPandas()
        my_model.fit(pandas_sales_df)
        future_dates = my_model.make_future_dataframe(periods=7, freq='D',include_history=True)
        forecast = my_model.predict(future_dates)
        final_forecast = forecast[['ds', 'yhat']].copy()
        final_forecast['STATION'] = stations['STATION'].iloc[station]
        final_forecast_spark = spark.createDataFrame(final_forecast)
        final_forecast_spark = final_forecast_spark.join(sales_df,['ds'])
        loaded_stations_df.append(final_forecast_spark)
    except Exception as e:
        print(f"An error occured: {e}")

# COMMAND ----------

merged_forecasted_sales_df = loaded_stations_df[0]
for x in range(1,len(loaded_stations_df)):
    merged_forecasted_sales_df = merged_forecasted_sales_df.unionAll(loaded_stations_df[x])

# COMMAND ----------

merged_forecasted_sales_df = merged_forecasted_sales_df.withColumnRenamed('ds','FORECASTED_SALES_DATE').withColumnRenamed('yhat','FORCASTED_DAILY_SALES_VOLUME').withColumnRenamed('y','ACTUAL_VOLUME_SOLD').withColumn("FORECASTED_SALES_DATE",date_format("FORECASTED_SALES_DATE","yyyy-MM-dd"))

# COMMAND ----------


merged_forecasted_sales_df = (merged_forecasted_sales_df
                               .withColumn('RUN_DATE_ID', lit(run_date_int).cast('int'))
                               .withColumn("INSERT_LOAD_TIME_ID", lit(run_id).cast('int'))
                               .withColumn("INSERT_LOAD_TIME", lit(current_time).cast('timestamp')))

# COMMAND ----------

unique_dates = merged_forecasted_sales_df.select("FORECASTED_SALES_DATE").distinct().rdd.flatMap(lambda x: x).collect()

dates_processed = [] 
for dates_ in unique_dates : 
    dates_processed.append(dates_)

# COMMAND ----------

dates_processed_str = ",".join(str(i) for i in dates_processed)
partition_predicate = f"FORECASTED_SALES_DATE IN ({dates_processed_str}) "
#this is to force caching process
print(merged_forecasted_sales_df.count())

# COMMAND ----------

"""WRITTING INTO GOLD"""
(merged_forecasted_sales_df.write.format("delta")
 .partitionBy("FORECASTED_SALES_DATE")
 .option("replaceWhere",  f"{partition_predicate}")
.mode("overwrite").save(model_path))

merged_forecasted_sales_df.unpersist(blocking=True)
