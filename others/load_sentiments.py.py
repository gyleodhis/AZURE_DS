# Databricks notebook source
from pyspark.sql.functions import trim, col, split, lit, lower ,regexp_replace

# COMMAND ----------

storage_account_name = "azwksdatalake2f2ph6.blob.core.windows.net"
container_name = "azsynapsewks2f2ph6"
sas_token = "sp=racwdlmeop&st=2023-06-02T15:35:11Z&se=2027-02-02T00:35:11Z&sv=2022-11-02&sr=c&sig=nyokEOZ53U%2BZBPgXyCVyrnDlYddv7Yjxsa09ZCKlqCA%3D"
storage_account_key = "E9F0Ug8s74PmO8bLdbSls7cPRdpYc+Ygo+Lguow9wrQATv4WUrSzU9Cjy7q/u9bYWK3zRhhC6Yge+AStebAQAw=="

# COMMAND ----------

spark.conf.set(
f"fs.azure.account.key.{storage_account_name}", f"{storage_account_key}"
)

# COMMAND ----------

spark.conf.set(
f"fs.azure.sas.{container_name}.{storage_account_name}",
f"{sas_token}"
)

# COMMAND ----------

tweets_df = (spark.read.format('csv')
    .option('header', True)
    .option("inferSchema", "true")
    .option("sep", "|")
    .option("multiline",True)
    .option("escape","\"")
    .load(f"wasbs://{container_name}@{storage_account_name}/landing/twitter/cleaned_tweets.csv")
.withColumnRenamed("_c0","tweet_id"))

# COMMAND ----------

tweets_df.display()

# COMMAND ----------


import pyspark.sql.functions as F
# cleaned_tweets_df = tweets_df.select([F.col(cols).alias(regexp_replace(trim(cols.lower())," ","_")) for cols in tweets_df.columns])
cleaned_tweets_df = tweets_df.select([col(cols).alias(cols.replace(' ', '_').lower()) for cols in tweets_df.columns])
cleaned_tweets_df.display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

cleaned_tweets_df.columns

# COMMAND ----------

(tweets_df.withColumnRenamed("_c0","tweet_id")
 .select()

# COMMAND ----------














# COMMAND ----------

https://azwksdatalake2f2ph6.blob.core.windows.net/azsynapsewks2f2ph6/landing/twitter/cleaned_tweets.csv

# COMMAND ----------


