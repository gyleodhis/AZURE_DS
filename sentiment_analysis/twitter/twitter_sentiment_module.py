# Databricks notebook source
import tweepy as tw
import numpy as np
import pandas as pd
import re
# import time
import plotly.express as px
import pymongo
from textblob import TextBlob
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import nltk

from pyspark.sql.functions import  col, lower , concat_ws ,when,upper ,split,coalesce,trim,regexp_replace
from difflib import get_close_matches


from datetime import datetime as dt , timedelta

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient



# COMMAND ----------

today = dt.now().date()
today_minus_1 = today - timedelta(days=50)

dbutils.widgets.text("current_date",  today_minus_1.isoformat())
# get parameters
load_date = dbutils.widgets.get("current_date")
load_date = today_minus_1.isoformat() if load_date == "None" else load_date 

print(f"Load date: {load_date}") 

# COMMAND ----------

nltk.download('stopwords')

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

synapse_connection_string = secret_client.get_secret("synapse-con-str").value
storage_account_key = secret_client.get_secret("storage-account-key").value
storage_sas_token = secret_client.get_secret("sas-token").value
cosmos_connection_string  = secret_client.get_secret("cosmos-conn-str").value


# COMMAND ----------

#silver path
container_var = f"wasbs://{container_name}@{storage_account_name}/"
silver_path = f"{container_var}silver/mapping/state/"

# COMMAND ----------

myclient = pymongo.MongoClient(cosmos_connection_string)
mydb = myclient["socialdb"]
mycol = mydb["nnpc"]
stop_words = set(stopwords.words('english'))

# COMMAND ----------

def get_tweets(a=5000):
    twets = pd.DataFrame(mycol.find({"load_date": f"{load_date}"},{})).drop(['_id','Tweet_id'], axis=1)
    twets['Accout Creation'] = pd.to_datetime(twets['Accout Creation']).dt.date
    twets['Tweet Time'] = pd.to_datetime(twets['Tweet Date']).dt.time
    twets['Tweet Date'] = pd.to_datetime(twets['Tweet Date']).dt.date
    twets = twets.drop_duplicates()
    twets.sort_values(by=['Tweet Date'], inplace=True)
    return twets.head(a)

# COMMAND ----------

def clean_tweet():
    '''
    Utility function to clean tweet text by removing links, special characters
    using simple regex statements.
    '''
    df_tweet = get_tweets(10)[['Tweet Text']]
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", df_tweet).split())

# COMMAND ----------

def remove_pattern(input_txt, pattern):
    r = re.findall(pattern, input_txt)
    for i in r:
        input_txt = re.sub(i, '', input_txt)
    return input_txt
    

# COMMAND ----------

def clean_tweets(a=5000):
    df_tweet = get_tweets(5000)
    """Remove twitter user handles"""
    df_tweet['df_Tweet'] = np.vectorize(remove_pattern)(df_tweet['Tweet Text'], "@[\w]*")
    """Remove Punctuations and special characters."""
    df_tweet['df_Tweet'] = df_tweet['df_Tweet'].str.replace("[^a-zA-Z#]", " ",regex=True)
    """Remove links."""
    df_tweet['df_Tweet'] = df_tweet['df_Tweet'].apply(lambda x: re.split('https:\/\/.*', str(x))[0])
    """Let us now remove all short words as they are usally connectors and have less meaning."""
    df_tweet['df_Tweet'] = df_tweet['df_Tweet'].apply(lambda x: ' '.join([w for w in x.split() if w not in (stop_words)]))
    df_tweet['df_Tweet'] = df_tweet['df_Tweet'].apply(lambda x: ' '.join([w for w in x.split() if len(w)>2]))
    df_tweet['df_Tweet'] = df_tweet['df_Tweet'].str.lower()
    """Add total number of longeractions a tweet has had"""
    df_tweet['longeractions'] = df_tweet[['Favorite Count','Retweets']].sum(axis=1)
    return df_tweet.head(a)

# COMMAND ----------

def sentiment_calc(text):
    try:
        return TextBlob(text).sentiment.polarity
    except:
        return None

# COMMAND ----------

def get_tweet_sentiment(a=5000):
    df_twit = clean_tweets(a)
    df_twit['sentiment'] = round(df_twit['Tweet Text'].apply(sentiment_calc),1)
    return df_twit.head(a)



# COMMAND ----------

tweets_df = spark.createDataFrame(get_tweet_sentiment(500000).astype(str))

# COMMAND ----------

#standardize column names 
cleaned_tweets_df = tweets_df.select([col(cols).alias(cols.replace(' ','_').lower()) for cols in tweets_df.columns])

# COMMAND ----------

cleaned_tweets_df = (cleaned_tweets_df
                            .withColumn("country1",trim(upper(split(col("location"),",")[1])))
                            .withColumn("country",trim(upper(split(col("location")," ")[1])))
                            .withColumn("location1",trim(upper(split(col("location"),",")[0])))
                            .withColumn("location",trim(upper(split(col("location1")," ")[0])))
                            .withColumn("country", coalesce("country1","country")) 
                      ).na.fill({'country': 'UNKNOWN','location':'UNKNOWN'})
cleaned_tweets_df = cleaned_tweets_df.drop("country1","location1")

# COMMAND ----------

cleaned_tweets_df = cleaned_tweets_df.selectExpr(
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
    'CAST(language AS string) tweet_language', 
    'CAST(following AS string) following', 
    'CAST(follow_request_sent AS string) follow_request_sent ',  
    'CAST(profile AS string) profile ', 
    'CAST(statuses_count AS string) statuses_count', 
    'CAST(location AS string) location', 
    'CAST(country AS string) country', 
    'CAST(filter_col AS string) filter_col', 
    'CAST(tweet_time AS string) tweet_time',
    'CAST(df_tweet AS string) df_tweet',     
    'CAST(longeractions AS string) longeractions',
    'CAST(sentiment AS string) sentiment',
    'CAST(load_date AS string) load_date'
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
