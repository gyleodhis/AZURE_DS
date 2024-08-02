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
synapse_temp_dir = f"wasbs://{container_name}@{storage_account_name}/synapse/temp/"
config_path = f"wasbs://{container_name}@{storage_account_name}/config/"
table_name = "tweets_sentiment"

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

sap_loading_df = (spark.read.format('csv')
    .option('header', True)
    .option("inferSchema", "true")
    .option("sep", ",")
    .load(f"wasbs://{container_name}@{storage_account_name}/landing/product_demand/SAP_LOADING_REPORTS_AUG_2022.csv"))

dodo_stations_df = (spark.read.format('csv')
    .option('header', True)
    .option("inferSchema", "true")
    .option("sep", ",")   
    .load(f"wasbs://{container_name}@{storage_account_name}/landing/product_demand/DODO_Stations_order_reports.csv")
    
    )
dodo_stations_df = (dodo_stations_df.withColumnRenamed("Delivery Doc. Number","Delivery_Doc_Number")
                        .withColumnRenamed("Shipment Doc. Number","Shipment_Doc_Number")
                        .withColumnRenamed("Delivery Truck Plate Number","Delivery_Truck_Plate_Number")
                        .withColumnRenamed("Clearing Doc. Number","Clearing_Doc_Number"))


"""STATION METER READINGS"""                       
codo_station_meter_readings_df = (spark.read.format('csv')
    .option('header', True)
    .option("inferSchema", "true")
    .option("sep", ",")   
    .load(f"wasbs://{container_name}@{storage_account_name}/landing/product_demand/CODO_Station_Meter_Readings.csv"))

"""STOCK VOLUME"""
codo_station_stock_volume_df = (spark.read.format('csv')
    .option('header', True)
    .option("inferSchema", "true")
    .option("sep", ",")   
    .load(f"wasbs://{container_name}@{storage_account_name}/landing/product_demand/CODO_Stations_Stock_volume.csv"))

"""TRUCK DELIVERIES"""
codo_station_truck_deliveries_df = (spark.read.format('csv')
    .option('header', True)
    .option("inferSchema", "true")
    .option("sep", ",")   
    .load(f"wasbs://{container_name}@{storage_account_name}/landing/product_demand/CODO_Stations_trucks_deliveries.csv"))


# COMMAND ----------

# DBTITLE 1,Standidization Data  METER READING FILE
codo_station_meter_readings_df = codo_station_meter_readings_df.select([col(cols).alias(cols.replace(' ','_').lower()) for cols in codo_station_meter_readings_df.columns])

codo_station_meter_readings_df.display()



# COMMAND ----------

# DBTITLE 1,Standidization Data  SAP FILE
sap_loading_df = sap_loading_df.select([col(cols).alias(cols.replace(' ','_').lower()) for cols in sap_loading_df.columns])

sap_loading_df.display()

# COMMAND ----------

# DBTITLE 1,Standidization Data  DODO FILE
dodo_stations_df =  (dodo_stations_df.select([col(cols).alias(cols.replace('.','').replace(' ','_').lower()) for cols in dodo_stations_df.columns])
                     ).withColumn("shipment_doc_number",regexp_replace(col("shipment_doc_number"),"'",""))

dodo_stations_df.display()

# COMMAND ----------

# DBTITLE 1,Standidization Data  CODO STOCK VOLUME FILE
codo_station_stock_volume_df = codo_station_stock_volume_df.select([col(cols).alias(cols.replace(' ','_').lower()) for cols in codo_station_stock_volume_df.columns])

codo_station_stock_volume_df.display()

# COMMAND ----------

# DBTITLE 1,Standidization Data  CODO TRUCK DELIVERIES FILE
codo_station_truck_deliveries_df = codo_station_truck_deliveries_df.select([col(cols).alias(cols.replace(' ','_').lower()) for cols in codo_station_truck_deliveries_df.columns])
codo_station_truck_deliveries_df.display()

# COMMAND ----------

# DBTITLE 1,JOIN SAP & DODO BASED ON STATION OR CUSTOMER NAME
joined_station_dodo_and_sap_df = dodo_stations_df.join(sap_loading_df,lower(sap_loading_df.delivery_station) == lower(dodo_stations_df.customer_name),"inner")

# COMMAND ----------

joined_station_dodo_and_sap_df.display()

# COMMAND ----------

# DBTITLE 1,JOIN SAP & DODO WITH TRUCK DELIVERY BASED ON STATION NAME

joined_station_dodo_and_sap_with_deliveries_df = joined_station_dodo_and_sap_df.join(codo_station_truck_deliveries_df,lower(joined_station_dodo_and_sap_df.delivery_station) == lower(codo_station_truck_deliveries_df.station),"inner")

joined_station_dodo_and_sap_with_deliveries_df.display()


# COMMAND ----------

# DBTITLE 1,JOIN METER READING & STATION VOLUME BASED ON RECORD ID
joined_codo_station_meter_readings_stock_volue_record_id = codo_station_meter_readings_df.join(codo_station_stock_volume_df,codo_station_stock_volume_df.record_id == codo_station_meter_readings_df.record_id,"inner")
joined_codo_station_meter_readings_stock_volue_record_id.display()

# COMMAND ----------

# DBTITLE 1,JOIN METER READING & STATION VOLUME BASED ON STATION NAME
joined_codo_station_meter_readings_stock_volue_station = codo_station_meter_readings_df.alias("meter_re").join(codo_station_stock_volume_df.alias("stock_vol"),codo_station_stock_volume_df.station == codo_station_meter_readings_df.station,"inner")
joined_codo_station_meter_readings_stock_volue_station.where("stock_vol.product = 'PMS' AND meter_re.product = 'PMS' AND stock_vol.station = 'ULTRA MEGA SAGAMU' ").display()

# COMMAND ----------

# DBTITLE 1,JOIN SAP & STATION VOLUME
joined_codo_station_stock_volue_with_sap = codo_station_meter_readings_df.join(sap_loading_df,sap_loading_df.sales_document == codo_station_meter_readings_df.record_id,"inner")
joined_codo_station_stock_volue_with_sap.where('record_id is not null').display()

# COMMAND ----------

# DBTITLE 1,JOIN STATION VOLUME & TRUCK DELIVERY BASED ON STATION NAME
# Joining with record_id returns no record at all while joining with station returns records but the records to do not look alike.
joined_codo_station_stock_with_truck_deliveries = codo_station_truck_deliveries_df.join(codo_station_stock_volume_df,codo_station_stock_volume_df.station == codo_station_truck_deliveries_df.station,"inner")
joined_codo_station_stock_with_truck_deliveries.display()

# COMMAND ----------

joined_station_dodo_and_codo_df = joined_station_dodo_and_sap_df.join(codo_station_meter_readings_df,lower(joined_station_dodo_and_sap_df.customer_name) == lower(codo_station_meter_readings_df.station),"inner")

