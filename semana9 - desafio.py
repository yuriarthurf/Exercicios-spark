#desafio
from pyspark.sql.functions import *

spark.conf.set("fs.azure.account.key.otaviodata.blob.core.windows.net", "Y+mxjw9LwK4gLybfLoflcDnwIqaa1aVYR/INp/G81SDiat1YqfaRV+7c2Qh9eAXKgZLa9U+Q9XaZEE03pj1OJQ==")
df = spark.read.csv('wasbs://yuri-rawdata@otaviodata.blob.core.windows.net/twitter/NoThemeTweets.csv/')
emojis_extras = [':p',':P',':-)',': )', ':Â´)', ':´)']
emojis_felizes = [':D', ':]', ':)']
emojis_tristes = [':(', ':[', ':{']

df = df.withColumn("Sentimento", when(col('_c1').rlike("\\:\\)"),"Positivo").when(col('_c1').rlike(":D"),"Positivo").when(col('_c1').rlike(":]"),"Positivo").when(col('_c1').rlike(":\\("),"Negativo").when(col('_c1').rlike(":\\["),"Negativo").when(col('_c1').rlike(":\\{"),"Negativo").otherwise('Emoji não encontrado')).withColumn("Emoji", when(col('_c1').rlike("\\:\\)"),":)").when(col('_c1').rlike(":D"),":D").when(col('_c1').rlike(":]"),":]").when(col('_c1').rlike(":\\("),":(").when(col('_c1').rlike(":\\["),":[").when(col('_c1').rlike(":\\{"),":{").when(col('_c1').rlike(":p"),":p").when(col('_c1').rlike(":P"),":P").when(col('_c1').rlike(":\\-\\)"),":-)").when(col('_c1').rlike(":Â\\´\\)"),":Â´)").when(col('_c1').rlike(":\\´\\)"),":´)").otherwise('Emoji não encontrado'))
display(df)

#df.write.json("wasbs://yuri-refdata@otaviodata.blob.core.windows.net/twitter/tweets_analisados_spark.parquet")