from pyspark.sql.functions import *

spark.conf.set( "fs.azure.account.key.otaviodata.blob.core.windows.net", "Y+mxjw9LwK4gLybfLoflcDnwIqaa1aVYR/INp/G81SDiat1YqfaRV+7c2Qh9eAXKgZLa9U+Q9XaZEE03pj1OJQ==" )
df = spark.read.json("wasbs://yuri-rawdata@otaviodata.blob.core.windows.net/30json/*.json")

df = df.withColumn("Sentimento", when(col('text').rlike('😀|😃|😄|😁|😆|😅|😂|🤣|😇|😉|😊|🙂|🙃|😋|😌|😍|🥰|😘|😗|😙|😚|🤪|😜|😝|😛|🤑|😎|🤓|🧐|🤠|🥳|🤗|🤡|😏|🤭|😳|🤩'),"Positivo").when(col('text').rlike('😵|🥴|😲|🤯|🤐|😷|🤕|🤒|🤮|🤢|🤧|🥵|🥶|😴|😈|👿|👹|👺|💩|😮|😱|😨|😰|😯|😦|😧|😢|😥|😪|🤤|😓|😭|😟|😠|😡|🤬|😔|😕|🙁|😬|🥺|😣|😖|😫|😩|🥱|😤😶|😐|😑|😒|🙄|🤨|🤔|🤫|😞|🤥'),"Negativo").otherwise('Emoji não encontrado')).withColumn("Emoji", when(col('text').rlike("🤔"),"🤔").when(col('text').rlike('🤧'),"🤧").when(col('text').rlike('👇'),"👇").when(col('text').rlike('🥵'),"🥵").when(col('text').rlike('😍'),"😍").when(col('text').rlike('🤣'),"🤣").when(col('text').rlike('🥰'),"🥰").when(col('text').rlike('🤗'),"🤗").otherwise('Emoji não encontrado'))
lista_negativos = '😵|🥴|😲|🤯|🤐|😷|🤕|🤒|🤮|🤢|🤧|🥵|🥶|😴|😈|👿|👹|👺|💩|😮|😱|😨|😰|😯|😦|😧|😢|😥|😪|🤤|😓|😭|😟|😠|😡|🤬|😔|😕|🙁|😬|🥺|😣|😖|😫|😩|🥱|😤😶|😐|😑|😒|🙄|🤨|🤔|🤫|😞|🤥'
lista_positivos = '😀|😃|😄|😁|😆|😅|😂|🤣|😇|😉|😊|🙂|🙃|😋|😌|😍|🥰|😘|😗|😙|😚|🤪|😜|😝|😛|🤑|😎|🤓|🧐|🤠|🥳|🤗|🤡|😏|🤭|😳|🤩'

df = df['created_at','id', 'text', 'Sentimento', 'Emoji']
display(df)
df.write.parquet("wasbs://yuri-refdata@otaviodata.blob.core.windows.net/twitter3/tweets_analisados_spark_emojis_wpp.parquet")