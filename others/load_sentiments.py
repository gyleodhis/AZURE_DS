# Databricks notebook source
from pyspark.sql.functions import  col, lower , concat_ws

# COMMAND ----------

storage_account_name = "azwksdatalake2f2ph6.blob.core.windows.net"
container_name = "azsynapsewks2f2ph6"
sas_token = "sp=racwdlmeop&st=2023-06-02T15:35:11Z&se=2027-02-02T00:35:11Z&sv=2022-11-02&sr=c&sig=nyokEOZ53U%2BZBPgXyCVyrnDlYddv7Yjxsa09ZCKlqCA%3D"
storage_account_key = "E9F0Ug8s74PmO8bLdbSls7cPRdpYc+Ygo+Lguow9wrQATv4WUrSzU9Cjy7q/u9bYWK3zRhhC6Yge+AStebAQAw=="

synapse_connection_string = "jdbc:sqlserver://azsynapsewks2f2ph6-ondemand.sql.azuresynapse.net:1433;database=rtiEnterpriseDWDev;user=aztrisynapseadmin@azsynapsewks2f2ph6;password=Hereisthelastdance2023$$;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;"

synapse_temp_dir = f"wasbs://{container_name}@{storage_account_name}/synapse/temp/"

table_name = "tweets_sentiment"

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
    .option("multiline", "true")
    .option("escape","\"")
    .option("sep", "|")
    .load(f"wasbs://{container_name}@{storage_account_name}/landing/twitter/cleaned_tweets.csv")
.withColumnRenamed("_c0","tweet_id"))


# COMMAND ----------

#standardize column names 
cleaned_tweets_df = tweets_df.select([col(cols).alias(cols.replace(' ','_').lower()) for cols in tweets_df.columns])


# COMMAND ----------

cleaned_tweets_df = cleaned_tweets_df.withColumn("df_tweet",concat_ws(",",col("df_tweet")))

# COMMAND ----------

cleaned_tweets_df = cleaned_tweets_df.selectExpr(
    'CAST(tweet_id AS string) tweet_id ', 
    'CAST(author AS string) author ',
    'CAST(tweet_date AS string) tweet_date',
    'CAST(follower_count AS string) follower_count', 
    'CAST(friends_count AS string) friends_count', 
    'CAST(account_verified AS string) account_verified', 
    'CAST(favorite_count AS string) favorite_count ', 
    'CAST(accout_creation AS string) accout_creation', 
    'CAST(retweets AS string) retweets', 
    'CAST(tweet_text AS string) tweet_text',
    'CAST(userdesc AS string) userdesc',
    'CAST(device AS string) device',
    'CAST(timezone AS string) timezone', 
    'CAST(language AS string) language', 
    'CAST(following AS string) following', 
    'CAST(follow_request_sent AS string) follow_request_sent ',  
    'CAST(profile AS string) profile ', 
    'CAST(statuses_count AS string) statuses_count', 
    'CAST(location AS string) location', 
    'CAST(filter_col AS string) filter_col', 
    'CAST(tweet_time AS string) tweet_time',
    'CAST(df_tweet AS string) df_tweet',     
    'CAST(interactions AS string) interactions',
    'CAST(sentiment AS string) sentiment'
)

# COMMAND ----------

(cleaned_tweets_df.write
                    .format('com.databricks.spark.sqldw')
                    .option('url', synapse_connection_string)
                    .option('forwardSparkAzureStorageCredentials', 'true')
                    .option('dbTable', f"{table_name}")
                    .option('tempDir', synapse_temp_dir)
                    .mode('append')
                    .save())
