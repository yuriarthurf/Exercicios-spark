import tweepy
import json
import os, uuid, sys
import time
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings
from pyspark.sql import SparkSession
from itertools import zip_longest

spark.conf.set( "fs.azure.account.key.otaviodata.dfs.core.windows.net", "Y+mxjw9LwK4gLybfLoflcDnwIqaa1aVYR/INp/G81SDiat1YqfaRV+7c2Qh9eAXKgZLa9U+Q9XaZEE03pj1OJQ==" )
dbutils.fs.ls("abfss://yuri-rawdata@otaviodata.dfs.core.windows.net/twitter")

auth = tweepy.OAuthHandler('v7Jax2EW7oWgRgkZeoBFbYar0', 'V9iskJFrIgMA46RtEWWTo0SlEUYcihUL7ApMSuUo3xyIhnoMsm')
auth.set_access_token('1400233273964515331-Dh92kRooJ5ZImdtNiiBlXS6bo9X3We',
                      'V0uSqGgXFGRiOIkc6bOmAyMB30ERNGjgB2jNitX9jSr3V')
api = tweepy.API(auth)

jsons = 0
i = 0
while i < 30:
  final_file = ''
  for tweet in api.search(q="Bolsonaro",rpp=100,count=100,result_type="recent",include_entities=True,lang="pt"):
      final_file += json.dumps(tweet._json)
  final_file = final_file.replace('}{', '},{')
  final_file = '[' + final_file + ']'
  newname = 'twitter_'+str(i)+'.json'
  i += 1
  def initialize_storage_account(storage_account_name, storage_account_key):
      try:
          global service_client

          service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
              "https", storage_account_name), credential=storage_account_key)

      except Exception as e:
          print(e)


  def upload_file_to_directory():
      try:

          file_system_client = service_client.get_file_system_client(file_system="yuri-rawdata")

          directory_client = file_system_client.get_directory_client("30json")

          file_client = directory_client.create_file(newname)
          local_file = final_file

          file_contents = local_file

          file_client.append_data(data=file_contents, offset=0, length=len(file_contents))

          file_client.flush_data(len(file_contents))

      except Exception as e:
          print(e)

  initialize_storage_account('otaviodata', 'Y+mxjw9LwK4gLybfLoflcDnwIqaa1aVYR/INp/G81SDiat1YqfaRV+7c2Qh9eAXKgZLa9U+Q9XaZEE03pj1OJQ==')
  upload_file_to_directory()

df = spark.read.json("wasbs://yuri-rawdata@otaviodata.blob.core.windows.net/30json/twitter_0.json")
df = df.withColumn("Sentimento", when(col('text').rlike("\\:\\)"),"Positivo").when(col('text').rlike(":D"),"Positivo").when(col('text').rlike(":]"),"Positivo").when(col('text').rlike(":\\("),"Negativo").when(col('text').rlike(":\\["),"Negativo").when(col('text').rlike(":\\{"),"Negativo").otherwise('Emoji não encontrado')).withColumn("Emoji", when(col('text').rlike("\\:\\)"),":)").when(col('text').rlike(":D"),":D").when(col('text').rlike(":]"),":]").when(col('text').rlike(":\\("),":(").when(col('text').rlike(":\\["),":[").when(col('text').rlike(":\\{"),":{").when(col('text').rlike(":p"),":p").when(col('text').rlike(":P"),":P").when(col('text').rlike(":\\-\\)"),":-)").when(col('text').rlike(":Â\\´\\)"),":Â´)").when(col('text').rlike(":\\´\\)"),":´)").otherwise('Emoji não encontrado'))
df = df['created_at','id', 'text', 'Sentimento', 'Emoji']