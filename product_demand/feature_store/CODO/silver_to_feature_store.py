# Databricks notebook source
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from pyspark.sql.functions import  col, lit,countDistinct,sum as fsum, max as fmax ,count,min as fmin, median,date_format,to_date, lag, datediff ,when,explode, sequence
from pyspark.sql.window import Window
from datetime import datetime, timedelta

import time


# COMMAND ----------

#Set up credentials
credential = DefaultAzureCredential()
secret_client = SecretClient(vault_url="https://azkeyvault2f2ph6.vault.azure.net/", credential=credential)

storage_account_name = "azwksdatalake2f2ph6.blob.core.windows.net"
container_name = "azsynapsewks2f2ph6"
synapse_temp_dir = f"wasbs://{container_name}@{storage_account_name}/synapse/temp/"

# COMMAND ----------

#Retrieve storage credentals

storage_account_key = secret_client.get_secret("storage-account-key").value
storage_sas_token = secret_client.get_secret("sas-token").value

synapse_connection_string = secret_client.get_secret("synapse-con-str").value

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
dbutils.widgets.text("start_date",(current_time-timedelta(1)).strftime("%Y-%m-%d"))
dbutils.widgets.text("end_date",(current_time-timedelta(1)).strftime("%Y-%m-%d"))

start_date_str = dbutils.widgets.get("start_date")
end_date_str = dbutils.widgets.get("end_date")

run_id = int(time.mktime(current_time.timetuple()))
run_date_int = datetime.fromtimestamp(run_id).strftime("%Y%m%d")

start_date = datetime.strptime(start_date_str,"%Y-%m-%d").strftime("%Y%m%d")
end_date = datetime.strptime(end_date_str,"%Y-%m-%d").strftime("%Y%m%d")

print("Start date: " + str(start_date))
print("End date: " + str(end_date))
print("run id: " + str(run_date_int))

# COMMAND ----------

# define paths 
container_var = f"wasbs://{container_name}@{storage_account_name}/"

codo_stock_silver_path = f"{container_var}silver/product_demand/codo_stations_stock_volume/"
codo_truck_silver_path = f"{container_var}silver/product_demand/codo_stations_truck_deliveries/"
codo_meter_silver_path = f"{container_var}silver/product_demand/codo_station_meter_reading/"

agg_gold_path = f"{container_var}gold/product_demand/codo_agg_feature_store/"
codo_raw_gold_path = f"{container_var}gold/product_demand/codo_raw_features/"



# COMMAND ----------

"""READING SILVER CODO DATA """                       
codo_stock_loading_df = (spark.read.format('delta')
    .load(codo_stock_silver_path).where(f"RUN_DATE_ID BETWEEN {start_date} AND {end_date}")
    )

codo_truck_loading_df = (spark.read.format('delta')
    .load(codo_truck_silver_path).where(f"RUN_DATE_ID BETWEEN {start_date} AND {end_date}")
    )

codo_meter_loading_df = (spark.read.format('delta')
    .load(codo_meter_silver_path).where(f"RUN_DATE_ID BETWEEN {start_date} AND {end_date}")
    )

# COMMAND ----------

#Join codo data based on estimation of delivery date and sales dates
windowSpec = Window.partitionBy("STATION","MATERIAL").orderBy(col("DELIVERY_DATE").desc())
cal_lag_df =codo_truck_loading_df.withColumn("DATE_LAG",lag("DELIVERY_DATE",1).over(windowSpec))
 
next_delivery_days_df =  (cal_lag_df
                         .withColumn("NEXT_DELIVERY_DAYS",datediff(to_date(cal_lag_df.DATE_LAG,"yyyyMMdd" ), to_date(cal_lag_df.DELIVERY_DATE,"yyyyMMdd" )) )
                         .withColumn("DATE_LAG",when(col("DATE_LAG").isNull(),col("DELIVERY_DATE"))
                                     .otherwise(col("DATE_LAG"))))

#generate sales date based on delivery date lag
generated_sales_date_df = next_delivery_days_df.withColumn("SALES_DATE", 
                   explode(sequence(to_date(next_delivery_days_df.DELIVERY_DATE,"yyyyMMdd" ), to_date(next_delivery_days_df.DATE_LAG,"yyyyMMdd" ))))

final_delivery_sales_estimation_df = (generated_sales_date_df.withColumn("SALES_DATE",date_format("SALES_DATE","yyyyMMdd" ))
                                      .withColumn("PRODUCT",col("MATERIAL"))
                                      )


joined_codo_data = (final_delivery_sales_estimation_df.alias("td_df")
                    .join(codo_stock_loading_df.alias("stl_df"),["STATION","PRODUCT","SALES_DATE"],"outer")
                    .join(codo_meter_loading_df.alias("ml_df"),["STATION","PRODUCT","SALES_DATE"],"outer")
                    )

joined_codo_data = joined_codo_data.select(
                    "STATION",
                    "PRODUCT",
                    "SALES_DATE",
                    "MATERIAL",
                    "QUANTITY",
                    col("td_df.STATUS").alias("TRUCK_DELIVERY_STATUS"),
                    "DELIVERY_DATE",
                    col("td_df.DATE_CREATED").alias("TRUCK_DELIVERY_DATE_CREATED"),
                    "DATE_LAG",
                    "NEXT_DELIVERY_DAYS",
                    "TANK_DESCRIPTION",
                    "DIP_READING",
                    col("stl_df.STATUS").alias("STOCK_VOLUME_STATUS"),
                    col("stl_df.DATE_CREATED").alias("STOCK_VOLUME_DATE_CREATED"),
                    "METER_DESCRIPTION",
                    "PREVIOUS_READING",
                    "CURRENT_READING",
                    "VOLUME",
                    "TYPE",
                    "UNIT_PRICE",
                    "SALES_VALUE",
                    "DEALER_COMMISSION",
                    "DEALER_COMMISSION_AMOUNT",
                    col("ml_df.STATUS").alias("METER_READING_STATUS"),
                    col("ml_df.DATE_CREATED").alias("METER_READING_DATE_CREATED")
)



# COMMAND ----------

max_deep_reading_codo_stock_loading_df = (codo_stock_loading_df.groupBy("STATION","TANK_DESCRIPTION")
                                          .agg(fmax("DIP_READING").alias("MAX_DEEP_READING"))
                                          .orderBy(col("MAX_DEEP_READING").desc())                                          
                                          )

vol_per_tank = (max_deep_reading_codo_stock_loading_df.select("STATION","TANK_DESCRIPTION","MAX_DEEP_READING"))


capcity_station_codo_stock_loading_df = (max_deep_reading_codo_stock_loading_df.groupBy("STATION")
                                         .agg(fsum("MAX_DEEP_READING").alias("TOTAL_STATION_CAPACITY"))
                                         .orderBy(col("TOTAL_STATION_CAPACITY").desc())
                                         )



unique_tanks_stock_loading_df = (codo_stock_loading_df
                                 .groupBy("STATION")
                                 .agg(countDistinct('TANK_DESCRIPTION').alias("TOTAL_NUMBER_OF_TANKS"))
                                 .orderBy(col("TOTAL_NUMBER_OF_TANKS").desc())
                                )




metrics_stock_volume_df = (unique_tanks_stock_loading_df.join(capcity_station_codo_stock_loading_df,
                                    ["STATION"] ,
                                    how = "inner")
                             .join(vol_per_tank,
                                    ["STATION"], 
                                    how = "inner"))


# COMMAND ----------


#Station with highest quantity ordered
new_order_truck_loading_df = (codo_truck_loading_df.groupBy("STATION","MATERIAL","DELIVERY_DATE","QUANTITY")
                                          .agg(count("QUANTITY").alias("COUNT_OF_NEW_ORDERS"))
                                          .orderBy(col("QUANTITY").desc())                                          
                                          )

current_order_truck_loading_df = (codo_truck_loading_df.groupBy("STATION","MATERIAL")
                                          .agg(fmax("DELIVERY_DATE").alias("DELIVERY_DATE"))
                                          .orderBy(col("DELIVERY_DATE").desc())                                          
                                          )

current_with_quantity_order_truck_loading_df = current_order_truck_loading_df.join(codo_truck_loading_df,
                                           ['STATION','MATERIAL','DELIVERY_DATE']
                                           ,how ='inner')                            

# COMMAND ----------

scarsity_df = current_with_quantity_order_truck_loading_df.alias("cur_w_qty").join(codo_meter_loading_df,
                                    (codo_meter_loading_df.STATION == current_with_quantity_order_truck_loading_df.STATION) &  
                                    (codo_meter_loading_df.PRODUCT == current_with_quantity_order_truck_loading_df.MATERIAL)                                    
                                    , how ='inner'
                            ).where("SALES_DATE > DELIVERY_DATE AND TYPE ='Sales'")

scarcity_df = (scarsity_df.groupBy("cur_w_qty.STATION","cur_w_qty.MATERIAL",col("DELIVERY_DATE").alias("CURRENT_DELIVERY_DATE"),col("QUANTITY").alias("CURRENT_DELIVERED_QUANTITY"))
                                          .agg(fsum("VOLUME").alias("TOTAL_VOLUME_SOLD_POST_CURRENT_DELIVERY_DATE"))
                                                                                    
                                          )
new_order_scarcity_df = scarcity_df.join(new_order_truck_loading_df,['STATION','MATERIAL'],how = 'outer').drop("cur_w_qty.QUANTITY")

# COMMAND ----------

truck_and_stock_volume_df = (new_order_scarcity_df.join(metrics_stock_volume_df,
                                    ["STATION"] ,
                                    how = "outer")
                             )

# COMMAND ----------

#Station with highest volume sold 
daily_volume_product_sold_df = (codo_meter_loading_df.groupBy("STATION","METER_DESCRIPTION","TYPE","SALES_DATE")
                                          .agg(  fsum("SALES_VALUE").alias("TOTAL_DAILY_SALES_VALUE")
                                                ,fsum("VOLUME").alias("TOTAL_DAILY_VOLUME_PRODUCT_SOLD")
                                               )                                     
                                                                                 
                                          )

max_volume_and_value_product_sold_df = (codo_meter_loading_df.groupBy("STATION","METER_DESCRIPTION","PRODUCT")
                                          .agg(fmax("VOLUME").alias("MAX_VOLUME_PRODUCT_SOLD")
                                               ,median("VOLUME").alias("MEDIAN_VOLUME_PRODUCT_SOLD")
                                               ,fmin("VOLUME").alias("MIN_VOLUME_PRODUCT_SOLD")
                                               ,fsum("SALES_VALUE").alias("TOTAL_SALES_VALUE")
                                               ,fmax("SALES_VALUE").alias("MAX_SALES_VALUE")
                                               ,median("SALES_VALUE").alias("MEDIAN_SALES_VALUE")
                                               ,fmin("SALES_VALUE").alias("MIN_SALES_VALUE")
                                               ,fmax("CURRENT_READING").alias("MAX_CURRENT_READING")
                                               ,median("CURRENT_READING").alias("MEDIAN_CURRENT_READING")
                                               ,fmin("CURRENT_READING").alias("MIN_CURRENT_READING")
                                               ,fmax("PREVIOUS_READING").alias("MAX_PREVIOUS_READING")
                                               ,median("PREVIOUS_READING").alias("MEDIAN_PREVIOUS_READING")
                                               ,fmin("PREVIOUS_READING").alias("MIN_PREVIOUS_READING")
                                               )                                                                                
                                          )

# COMMAND ----------

total_sales_and_volume_df = (daily_volume_product_sold_df.join(max_volume_and_value_product_sold_df,
                                    ["STATION","METER_DESCRIPTION"] ,
                                    how = "outer")
                             )

# COMMAND ----------

#Combine all codo features 
combined_codo_agg_features =  (truck_and_stock_volume_df.join(total_sales_and_volume_df,
                                    ["STATION"] ,
                                    how = "outer")
                             )

# COMMAND ----------

combined_codo_agg_features = combined_codo_agg_features.select(
                            "STATION",
                            "MATERIAL",
                            "DELIVERY_DATE",
                            "QUANTITY",
                            "COUNT_OF_NEW_ORDERS",
                            "TOTAL_NUMBER_OF_TANKS",
                            "TOTAL_STATION_CAPACITY",
                            "TANK_DESCRIPTION",
                            "MAX_DEEP_READING",
                            "METER_DESCRIPTION",
                            "TYPE",
                            "SALES_DATE",
                            "TOTAL_DAILY_SALES_VALUE",
                            "TOTAL_DAILY_VOLUME_PRODUCT_SOLD",
                            "TOTAL_VOLUME_SOLD_POST_CURRENT_DELIVERY_DATE",
                            "CURRENT_DELIVERY_DATE",
                            "CURRENT_DELIVERED_QUANTITY",
                            "PRODUCT",
                            "MAX_VOLUME_PRODUCT_SOLD",
                            "MEDIAN_VOLUME_PRODUCT_SOLD",
                            "MIN_VOLUME_PRODUCT_SOLD",
                            "TOTAL_SALES_VALUE",
                            "MAX_SALES_VALUE",
                            "MEDIAN_SALES_VALUE",
                            "MIN_SALES_VALUE",
                            "MAX_CURRENT_READING",
                            "MEDIAN_CURRENT_READING",
                            "MIN_CURRENT_READING",
                            "MAX_PREVIOUS_READING",
                            "MEDIAN_PREVIOUS_READING",
                            "MIN_PREVIOUS_READING"
                                    

)

# COMMAND ----------

#Add watermark columns 
joined_codo_data = (joined_codo_data
                       .withColumn('SALES_MONTH',  date_format(to_date("SALES_DATE","yyyyMMdd"),"yyyyMM").cast("int"))
                       .withColumn('DELIVERY_MONTH', date_format(to_date("DELIVERY_DATE","yyyyMMdd"),"yyyyMM").cast("int"))                      
                       .withColumn('RUN_DATE_ID', lit(run_date_int).cast('int'))
                       .withColumn("INSERT_LOAD_TIME_ID", lit(run_id).cast('int'))
                       .withColumn("INSERT_LOAD_TIME", lit(current_time).cast('timestamp')))

combined_codo_agg_features = (combined_codo_agg_features
                       .withColumn('SALES_MONTH',  date_format(to_date("SALES_DATE","yyyyMMdd"),"yyyyMM").cast("int"))
                       .withColumn('DELIVERY_MONTH', date_format(to_date("DELIVERY_DATE","yyyyMMdd"),"yyyyMM").cast("int"))                      
                       .withColumn('RUN_DATE_ID', lit(run_date_int).cast('int'))
                       .withColumn("INSERT_LOAD_TIME_ID", lit(run_id).cast('int'))
                       .withColumn("INSERT_LOAD_TIME", lit(current_time).cast('timestamp')))

# COMMAND ----------

unique_dates = combined_codo_agg_features.select("SALES_DATE").distinct().rdd.flatMap(lambda x: x).collect()

dates_processed = [] 
for dates_ in unique_dates : 
    dates_processed.append(dates_)

# COMMAND ----------

#Create a comma separated list
dates_processed_str = ",".join(str(i) for i in dates_processed)
# Adding partition predicate
partition_predicate = f"SALES_DATE IN ({dates_processed_str}) "

# COMMAND ----------

"""WRITTING CODO RAW COMBINED FEATURES"""
(joined_codo_data.write.format("delta")
 .partitionBy("SALES_DATE")
.mode("overwrite").save(codo_raw_gold_path))


"""WRITTING CODO AGG FEATURES"""
(combined_codo_agg_features.write.format("delta")
 .partitionBy("SALES_DATE")
 #.option("replaceWhere",  f"{partition_predicate}")
.mode("overwrite").save(agg_gold_path))

# COMMAND ----------

agg_codo_features = 'CODO_AGG_FEATURE_STORE'
raw_codo_features = 'CODO_RAW_FEATURES'


(joined_codo_data.write
                        .format('com.databricks.spark.sqldw')
                        .option('url', synapse_connection_string)
                        .option('forwardSparkAzureStorageCredentials', 'true')
                        .option('dbTable', f"{raw_codo_features}")
                        .option('tempDir', synapse_temp_dir)
                        .mode('overwrite')
                        .save())


(combined_codo_agg_features.write
                        .format('com.databricks.spark.sqldw')
                        .option('url', synapse_connection_string)
                        .option('forwardSparkAzureStorageCredentials', 'true')
                        .option('dbTable', f"{agg_codo_features}")
                        .option('tempDir', synapse_temp_dir)
                        .mode('overwrite')
                        .save())
                    
