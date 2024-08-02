# Databricks notebook source
# DBTITLE 1,Import modules
import json
import tweepy as tw
import numpy as np
import pandas as pd
import re
import os
import time
import plotly.express as px
import pymongo

from datetime import datetime as dt , timedelta
from dotenv import load_dotenv

load_dotenv()

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

# COMMAND ----------

# DBTITLE 1,Get today's date for the purpose of loading tweets
today = dt.now().date()

dbutils.widgets.text("current_date",  today.isoformat())
# get parameters
current_date = dbutils.widgets.get("current_date")
current_date = today if current_date == "None" else current_date 

print(f"Load date: {current_date}") 


# COMMAND ----------

# DBTITLE 1,KeyVault Credentials
credential = DefaultAzureCredential()
secret_client = SecretClient(vault_url="https://azkeyvault2f2ph6.vault.azure.net/", credential=credential)

# COMMAND ----------

# DBTITLE 1,Synapse Credentials
storage_account_key = secret_client.get_secret("storage-account-key").value
storage_sas_token = secret_client.get_secret("sas-token").value
cosmos_connection_string  = secret_client.get_secret("cosmos-conn-str").value



# COMMAND ----------

storage_account_name = "azwksdatalake2f2ph6.blob.core.windows.net"
container_name = "azsynapsewks2f2ph6"
synapse_temp_dir = f"wasbs://{container_name}@{storage_account_name}/synapse/temp/"
config_path = f"wasbs://{container_name}@{storage_account_name}/config/"

# COMMAND ----------

# DBTITLE 1,Twitter Secrets

#Twitter credentials
twitter_access_token_secret  = secret_client.get_secret("twitter-access-token-secret").value
twitter_access_token  = secret_client.get_secret("twitter-access-token").value
twitter_api_secret  = secret_client.get_secret("twitter-api-secret").value
twitter_api_key   = secret_client.get_secret("twitter-api-key").value

consumer_key = twitter_api_key
consumer_secret = twitter_api_secret
access_token = twitter_access_token
access_token_secret = twitter_access_token_secret
auth = tw.OAuth1UserHandler(
    consumer_key,
    consumer_secret,
    access_token,
    access_token_secret
)

api = tw.API(auth,wait_on_rate_limit=True)

# COMMAND ----------

# DBTITLE 1,Cosmos Details
myclient = pymongo.MongoClient(cosmos_connection_string)
"""DATABASE NAME"""
mydb = myclient["socialdb"]
"""TWITTER COLLECTIONS NAME"""
mycol = mydb["nnpc"]
"""TOPICS COLLECTION"""
keywords_col = mydb["keywords"]

# COMMAND ----------

# DBTITLE 1,Fetch all available topics
def getKeyWord(a ='k'):
    keyword = pd.DataFrame(keywords_col.find({},{ "_id": 0, "twitter": 1}))
    keyword = keyword.explode('twitter')
    if a=='k':
        return keyword.shape[0]
    elif a < keyword.shape[0]:
        return keyword['twitter'].iloc[a]
    else:
        return "Incorrect value or out of range value passed"
    """Get total number of records in database"""
df_len = getKeyWord()
KeyWords = range(df_len)

# COMMAND ----------

# DBTITLE 1,Ingest Tweets
tweet_dataset = pd.DataFrame(columns=['Tweet_id','Author', 'Tweet Date','Follower Count','Friends Count', 'Account Verified',
                                      'Favorite Count','Accout Creation','Retweets', 'Tweet Text','UserDesc',
                                      'Device','Timezone','Language','Following','Follow Request Sent','Profile','statuses_count',
                                      'Location','filter_col','load_date'
                                     ])
for y in KeyWords:
    for tweet in tw.Cursor(api.search_tweets,tweet_mode='extended',until=current_date, q=getKeyWord(y)).items(521):
        appending_dataframe = pd.DataFrame([[tweet.id,tweet.author.screen_name, tweet.created_at, tweet.user.followers_count,
                                             tweet.user.friends_count,tweet.user.verified, tweet.favorite_count,
                                             tweet.user.created_at, tweet.retweet_count,tweet.full_text,
                                             tweet.user.description,tweet.source,tweet.user.time_zone,tweet.lang,
                                             tweet.user.following,tweet.user.follow_request_sent,tweet.user.profile_image_url_https,
                                            tweet.user.statuses_count,tweet.user.location,getKeyWord(y),current_date]],
                                           columns=['Tweet_id','Author', 'Tweet Date','Follower Count','Friends Count','Account Verified', 
                                                    'Favorite Count','Accout Creation','Retweets', 'Tweet Text',
                                                    'UserDesc','Device','Timezone','Language','Following','Follow Request Sent',
                                                   'Profile','statuses_count','Location','filter_col','load_date'])
        tweet_dataset = tweet_dataset.append(appending_dataframe)
        df_tweet_dataset = tweet_dataset.tail(1)
        if df_tweet_dataset.empty:
            continue
#         print(df_tweet_dataset[['Tweet_id','Author','filter_col']].head())
        x = df_tweet_dataset.to_dict('records')
        mycol.insert_many(x)

# COMMAND ----------

# DBTITLE 1,View a sample of inserted topics
show_Tweets = pd.DataFrame(mycol.find({"filter_col": "sustainable energy"},{ "Author": 1, "Tweet Date": 1,
                                                                           "filter_col":1,"load_date":1}))
show_Tweets
