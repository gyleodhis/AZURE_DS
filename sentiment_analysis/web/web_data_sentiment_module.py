# Databricks notebook source
import numpy as np
import pandas as pd
import re
import os
import time
import pymongo
from textblob import TextBlob
from datetime import datetime as dt , timedelta
from pyspark.sql.functions import  lit, col, lower , concat_ws , trim
# import nltk
load_dotenv()
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


# COMMAND ----------

today = dt.now()
today_minus_1 = today - timedelta(days=1)

dbutils.widgets.text("yesterday",  today_minus_1.isoformat())
dbutils.widgets.text("today",  today.isoformat())
# get parameters
start_date_dt = pd.to_datetime(dbutils.widgets.get("yesterday")).date()
end_date_dt = pd.to_datetime(dbutils.widgets.get("today")).date()

start_date = dt.combine(start_date_dt, dt.min.time())
end_date = dt.combine(end_date_dt, dt.min.time())

print(f"Start date: {start_date}")
print(f"End date: {end_date}")

# COMMAND ----------

credential = DefaultAzureCredential()
secret_client = SecretClient(vault_url="https://azkeyvault2f2ph6.vault.azure.net/", credential=credential)

# COMMAND ----------

storage_account_key = secret_client.get_secret("storage-account-key").value
storage_sas_token = secret_client.get_secret("sas-token").value
cosmos_connection_string  = secret_client.get_secret("cosmos-conn-str").value

# COMMAND ----------

storage_account_name = "azwksdatalake2f2ph6.blob.core.windows.net"
container_name = "azsynapsewks2f2ph6"
synapse_temp_dir = f"wasbs://{container_name}@{storage_account_name}/synapse/temp/"
config_path = f"wasbs://{container_name}@{storage_account_name}/config/"
container_var = f"wasbs://{container_name}@{storage_account_name}/"
table_name = 'web_sentiment'

# COMMAND ----------

synapse_connection_string = secret_client.get_secret("synapse-con-str").value
storage_account_key = secret_client.get_secret("storage-account-key").value
storage_sas_token = secret_client.get_secret("sas-token").value
cosmos_connection_string  = secret_client.get_secret("cosmos-conn-str").value

# COMMAND ----------

spark.conf.set(
f"fs.azure.account.key.{storage_account_name}", f"{storage_account_key}"
)

spark.conf.set(
f"fs.azure.sas.{container_name}.{storage_account_name}",
f"{storage_sas_token}"
)

# COMMAND ----------

myclient = pymongo.MongoClient(cosmos_connection_string)
"""DATABASE NAME"""
mydb = myclient["socialdb"]
"""WEB COLLECTIONS NAME"""
mycol = mydb["articles"]
"""TOPICS COLLECTION"""
#topics_col = mydb["keywords"]

# COMMAND ----------

#paths
gold_path = f"{container_var}gold/web_sentiment"

# COMMAND ----------

def get_news(a=5000):
    try:
        news = pd.DataFrame(mycol.find({"createdAt": {"$gte": start_date,"$lt": end_date}},{})).drop(['_id','__v','published'], axis=1)
        news['Created Date'] = pd.to_datetime(news['createdAt']).dt.date
        news = news.explode('keywords')
        return news.head(a)
    except Exception as e:
        return(f"error occured: {e}")

get_news(5)

# COMMAND ----------

def getWebTopic(a=10):
    df_topic = get_news()[['keywords']]
    df_topic = df_topic.groupby(["keywords"]).agg(Total=pd.NamedAgg(column="keywords", aggfunc="count")).reset_index("keywords")
    df_topic.sort_values(by=['Total'], inplace=True,ascending=False)
    df_topic['df_pct'] = (round(df_topic.Total / df_topic.Total.sum(),2))*100
    return df_topic
getWebTopic()

# COMMAND ----------

def remove_pattern(input_txt, pattern):
    r = re.findall(pattern, input_txt)
    for i in r:
        input_txt = re.sub(i, '', input_txt)
    return input_txt

# COMMAND ----------

def cleaned_news(a=1000):
    df_news = get_news(a)
    """Remove twitter user handles"""
    df_news['analyzed_words'] = np.vectorize(remove_pattern)(df_news['body'], "@[\w]*")
    """Remove Punctuations and special characters."""
    df_news['analyzed_words'] = df_news['analyzed_words'].str.replace("[^a-zA-Z#]", " ",regex=True)
    """Remove links."""
    df_news['analyzed_words'] = df_news['analyzed_words'].str.replace("[https://]", " ",regex=True)
    """Let us now remove all short words as they are usally connectors and have less meaning."""
    df_news['analyzed_words'] = df_news['analyzed_words'].apply(lambda x: ' '.join([w for w in x.split() if len(w)>7]))
    """Tokenizing the tweets"""
    df_news['analyzed_words'] = df_news['analyzed_words'].apply(lambda x: x.split())
    return df_news.head(a)
cleaned_news(5)

# COMMAND ----------

def sentiment_calc(text):
    try:
        return TextBlob(text).sentiment.polarity
    except:
        return None

# COMMAND ----------

def get_news_sentiment(a=5000):
    df_news = cleaned_news(a)
    df_news['sentiment'] = round(df_news['body'].apply(sentiment_calc),1)
    return df_news.head(a)
get_news_sentiment(5)

# COMMAND ----------

def getNegativeNews(a):
    df_news = get_news_sentiment()[['keywords','domain','sentiment','body','source']]
    df_news = df_news.loc[df_news['sentiment'] < 0]
    df_news.sort_values(by=['sentiment'], inplace=True,ascending=True)
    if a==1:
        x = df_news.head()
    else:
        x= df_news.shape[0]
    return x
getNegativeNews(1)

# COMMAND ----------

spark_news_df = spark.createDataFrame(get_news_sentiment().astype(str))
#standardize column names 
cleaned_spark_news_df = spark_news_df.select([col(cols).alias(cols.replace(' ','_').lower()) for cols in spark_news_df.columns])

# COMMAND ----------

print(cleaned_spark_news_df.count())

# COMMAND ----------

# Compare incoming data aganist whats existing and remove duplicates pages 
cleaned_spark_news_df = cleaned_spark_news_df.dropDuplicates(["keywords","source"])
gold_web_df = (spark.read.format('delta').load(gold_path))

deduped_cleaned_spark_news_df = cleaned_spark_news_df.alias("inc_df").join(gold_web_df.alias("gold_df"),
               ((trim(cleaned_spark_news_df.source)  == trim(gold_web_df.source)) & ( trim(cleaned_spark_news_df.keywords) == trim(gold_web_df.keywords))),
                how = 'left_anti')


# COMMAND ----------

deduped_cleaned_spark_news_df = deduped_cleaned_spark_news_df.selectExpr(
    'CAST(inc_df.keywords AS string) keywords ',
    'CAST(inc_df.domain AS string) site',
    'CAST(inc_df.source AS string) source', 
    'CAST(inc_df.createdat AS string) createdat', 
    'CAST(inc_df.updatedat AS string) updatedat', 
    'CAST(inc_df.created_date AS string) created_date', 
    'CAST(inc_df.analyzed_words AS string) analyzed_words', 
    'CAST(inc_df.sentiment AS string) sentiment'
)
new_data_count = deduped_cleaned_spark_news_df.count()
print(deduped_cleaned_spark_news_df.count())


# COMMAND ----------

if new_data_count > 0 :
        site_sources = deduped_cleaned_spark_news_df.select("site").distinct().rdd.flatMap(lambda x: x).collect()

        site_processed = [] 
        for sites in site_sources : 
            site_processed.append(f'"{sites}"')

        #Create a comma separated list
        site_processed_str = ",".join(str(i) for i in site_processed)

        # Adding partition predicate
        partition_predicate = f"site IN ({site_processed_str})  "

        print(partition_predicate)

# COMMAND ----------

if new_data_count > 0 :
    """WRITTING WEB_DATA"""
    (deduped_cleaned_spark_news_df.write.format("delta")
    .partitionBy("site")
    .option("replaceWhere",  f"{partition_predicate}")
    .mode("overwrite").save(gold_path))

# COMMAND ----------

if new_data_count > 0 :   
    deduped_cleaned_spark_news_df = deduped_cleaned_spark_news_df.dropDuplicates(["keywords","source"])
    
    (deduped_cleaned_spark_news_df.write
                        .format('com.databricks.spark.sqldw')
                        .option('url', synapse_connection_string)
                        .option('forwardSparkAzureStorageCredentials', 'true')
                        .option('dbTable', f"{table_name}")
                        .option('tempDir', synapse_temp_dir)
                        .mode('append')
                        .save())
