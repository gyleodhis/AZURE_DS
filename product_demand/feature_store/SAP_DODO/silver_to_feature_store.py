# Databricks notebook source
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from pyspark.sql.functions import  col,when, lit, trim ,  datediff , to_date  ,date_format
from datetime import datetime, timedelta

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

sap_silver_path = f"{container_var}silver/product_demand/sap_loading/"
dodo_silver_path = f"{container_var}silver/product_demand/dodo_stations_order_report/"
gold_path = f"{container_var}gold/product_demand/sap_dodo_feature_store/"

synapse_connection_string = secret_client.get_secret("synapse-con-str").value

# COMMAND ----------

"""READING SILVER SAP AND DODO DATA """                       
sap_loading_df = (spark.read.format('delta')
    .load(sap_silver_path)#.where(f"RUN_DATE_ID BETWEEN {start_date} AND {end_date}")
    )

dodo_loading_df = (spark.read.format('delta')
    .load(dodo_silver_path)#.where(f"RUN_DATE_ID BETWEEN {start_date} AND {end_date}")
    )

# COMMAND ----------

filtered_sap_loading_df = (sap_loading_df
                            .where("trim(lower(DISTRIBUTION_CHANNEL)) = 'affiliate station' OR trim(lower(DELIVERY_STATION)) like '% mega %'")
                            )

print(filtered_sap_loading_df.count())

# COMMAND ----------

joined_dodo_and_sap_df = (filtered_sap_loading_df.alias("sap")
                          .join(dodo_loading_df.alias("dodo"),
                                trim(filtered_sap_loading_df.SALES_QUOTATION) == trim(dodo_loading_df.SAP_QUOTATION_NUMBER),
                                how='outer').dropDuplicates())


print(joined_dodo_and_sap_df.count())


# COMMAND ----------

#calculate metrics 
"""
QUANTITY_ORDERED_TEMP_TOLERANCE_MAX : gets the maximum temparature tolerance of allwable 0.5% 
QUANTITY_ORDERED_TEMP_TOLERANCE_MIN  : gets the minimum temparature tolerance of allwable 0.5% 
"""
metrics_dodo_and_sap_df =  (joined_dodo_and_sap_df
                .withColumn("QUANTITY_ORDERED_TEMP_TOLERANCE_MIN", (joined_dodo_and_sap_df.QUANTITY_ORDERED*lit(0.95)))
                .withColumn("QUANTITY_ORDERED_TEMP_TOLERANCE_MAX", (joined_dodo_and_sap_df.QUANTITY_ORDERED*lit(1.05)))
                .withColumn("DELIVERY_DAYS",datediff(joined_dodo_and_sap_df.DELIVERY_DATE , to_date(joined_dodo_and_sap_df.SALES_DATE,"yyyyMMdd" )))
                .withColumn("ORDER_PROCESSING_DAYS",datediff(joined_dodo_and_sap_df.DISPATCH_DATE, to_date(joined_dodo_and_sap_df.SALES_DATE,"yyyyMMdd" )) )
                .withColumn("DELIVERY_DATE_DAY_OF_WEEK",date_format("DELIVERY_DATE", 'EEE')) 
                .withColumn("DISPATCH_DATE_DAY_OF_WEEK",date_format("DISPATCH_DATE", 'EEE') )
                .withColumn("COMPLETED_ON_DATE_DAY_OF_WEEK",date_format("COMPLETED_ON", 'EEE') )
                .withColumn("SALES_DATE_DAY_OF_WEEK",date_format(to_date(col("SALES_DATE"),"yyyyMMdd"), 'EEE') )
                
            )


# COMMAND ----------

# 1 = yes and 0 = no
metrics_dodo_and_sap_df =  (metrics_dodo_and_sap_df
                .withColumn("IS_QUANTITY_DELIVERED_WITHIN_TEMP_THRESHOLD", 
                            (when((col("QUANTITY_DELIVERED")>= col("QUANTITY_ORDERED_TEMP_TOLERANCE_MIN"))
                             & (col("QUANTITY_DELIVERED") <= col("QUANTITY_ORDERED_TEMP_TOLERANCE_MAX"))
                             , lit(1))
                             .otherwise(lit(0))))
                .withColumn("DELIVERY_IS_DELAYED", 
                            (when((col("DELIVERY_DAYS")>5)                             
                             , lit(1))
                             .otherwise(lit(0))))
                .withColumn("IS_PRODUCT_IN_TRANSIST", 
                            (when((col("DISPATCH_DATE").isNotNull() & col("DELIVERY_DATE").isNull() & col("COMPLETED_ON").isNull())                             
                             , lit(1))
                             .otherwise(lit(0))))
                
            )

# COMMAND ----------

metrics_dodo_and_sap_df = metrics_dodo_and_sap_df.select("SALES_QUOTATION",
                                    "sap.RRR_NUMBER",
                                    "SALES_DOCUMENT",
                                    "DISPATCH_NUMBER",
                                    "WAYBILL_NUMBER",
                                    "RESERVATION_NO",
                                    "TICKET_NUMBER",
                                    "LOADING_DEPOT_NAME",
                                    "PRODUCT_TYPE",
                                    "BATCH",
                                    "TRANSACTION_TYPE",                                    
                                    "TRANSACTION_UOM12",
                                    "LOADING_DATE",
                                    "MONTH",
                                    "REGISTERED_OWNER",
                                    "VEHICLE_NUMBER",
                                    "RECEIVING_DEPOT",
                                    "TRANSIT_TRUCK_STATUS",
                                    "QUANTITY_LOADED",
                                    "QUANTITY_ORDERED",                                    
                                    "QUANTITY_DELIVERED",
                                    "QUANTITY_DISPATCHED",
                                    "QUANTITY",
                                    "QUANTITY_ORDERED_TEMP_TOLERANCE_MIN",
                                    "QUANTITY_ORDERED_TEMP_TOLERANCE_MAX",
                                    "IS_QUANTITY_DELIVERED_WITHIN_TEMP_THRESHOLD",
                                    "SALES_DATE",
                                    "SALES_TIME",
                                    "SALES_DATE_DAY_OF_WEEK",
                                    "DISPATCH_DATE",
                                    "DISPATCH_DATE_DAY_OF_WEEK",
                                    "DELIVERY_DATE",
                                    "DELIVERY_DATE_DAY_OF_WEEK",
                                    "COMPLETED_ON", 
                                    "COMPLETED_ON_DATE_DAY_OF_WEEK",                                   
                                    "DELIVERY_DAYS",
                                    "DELIVERY_IS_DELAYED",
                                    "ORDER_PROCESSING_DAYS",
                                    "IS_PRODUCT_IN_TRANSIST",
                                    "SHIPMENT_STATUS",
                                    "DESTINATION",
                                    "DELIVERY_STATION",
                                    "DISTRIBUTION_CHANNEL",                                                                        
                                    "TRANSACTION_UOM28",
                                    "CREATED_BY",
                                    "CHANGED_BY",                                  
                                    "CUSTOMER_NAME",
                                    "SAP_QUOTATION_NUMBER",
                                    "PRODUCT",                                   
                                    col("dodo.RRR_NUMBER").alias("DODO_RRR_NUMBER"),
                                    "SAP_PAYMENT_NUMBER",
                                    "AMOUNT_PAID",
                                    "SAP_SALES_ORDER_NUMBER",
                                    "QUANTITY_SOLD",
                                    "DELIVERY_DOC_NUMBER",
                                    "SHIPMENT_DOC_NUMBER",
                                    "QUANTITY_RECEIVED",
                                    "DELIVERY_TRUCK_PLATE_NUMBER",
                                    "CLEARING_DOC_NUMBER",                                   
                                    "STATE",
                                    "REGION"                                   
                                    

)


# COMMAND ----------

#Add watermark columns 
metrics_dodo_and_sap_df = (metrics_dodo_and_sap_df                      
                       .withColumn('RUN_DATE_ID', lit(run_date_int).cast('int'))
                       .withColumn("INSERT_LOAD_TIME_ID", lit(run_id).cast('int'))
                       .withColumn("INSERT_LOAD_TIME", lit(current_time).cast('timestamp')))

# COMMAND ----------

unique_dates = metrics_dodo_and_sap_df.select("SALES_DATE").distinct().rdd.flatMap(lambda x: x).collect()

dates_processed = [] 
for dates_ in unique_dates : 
    dates_processed.append(dates_)

# COMMAND ----------

#Create a comma separated list
dates_processed_str = ",".join(str(i) for i in dates_processed)
# Adding partition predicate
partition_predicate = f"SALES_DATE IN ({dates_processed_str}) "

# COMMAND ----------

"""WRITTING DODO - SAP FEATURES"""
(metrics_dodo_and_sap_df.write.format("delta")
 .partitionBy("SALES_DATE")
.mode("overwrite").save(gold_path))

# COMMAND ----------

table_name = 'SAP_DODO_FEATURE_STORE'


(metrics_dodo_and_sap_df.write
                        .format('com.databricks.spark.sqldw')
                        .option('url', synapse_connection_string)
                        .option('forwardSparkAzureStorageCredentials', 'true')
                        .option('dbTable', f"{table_name}")
                        .option('tempDir', synapse_temp_dir)
                        .mode('overwrite')
                        .save())
                    
