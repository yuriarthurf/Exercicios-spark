from pyspark.sql.functions import *

spark.conf.set( "fs.azure.account.key.otaviodata.blob.core.windows.net", "Y+mxjw9LwK4gLybfLoflcDnwIqaa1aVYR/INp/G81SDiat1YqfaRV+7c2Qh9eAXKgZLa9U+Q9XaZEE03pj1OJQ==" )
df = spark.read.json("wasbs://yuri-rawdata@otaviodata.blob.core.windows.net/30json/*.json")

df = df.withColumn("Sentimento", when(col('text').rlike('๐|๐|๐|๐|๐|๐|๐|๐คฃ|๐|๐|๐|๐|๐|๐|๐|๐|๐ฅฐ|๐|๐|๐|๐|๐คช|๐|๐|๐|๐ค|๐|๐ค|๐ง|๐ค |๐ฅณ|๐ค|๐คก|๐|๐คญ|๐ณ|๐คฉ'),"Positivo").when(col('text').rlike('๐ต|๐ฅด|๐ฒ|๐คฏ|๐ค|๐ท|๐ค|๐ค|๐คฎ|๐คข|๐คง|๐ฅต|๐ฅถ|๐ด|๐|๐ฟ|๐น|๐บ|๐ฉ|๐ฎ|๐ฑ|๐จ|๐ฐ|๐ฏ|๐ฆ|๐ง|๐ข|๐ฅ|๐ช|๐คค|๐|๐ญ|๐|๐ |๐ก|๐คฌ|๐|๐|๐|๐ฌ|๐ฅบ|๐ฃ|๐|๐ซ|๐ฉ|๐ฅฑ|๐ค๐ถ|๐|๐|๐|๐|๐คจ|๐ค|๐คซ|๐|๐คฅ'),"Negativo").otherwise('Emoji nรฃo encontrado')).withColumn("Emoji", when(col('text').rlike("๐ค"),"๐ค").when(col('text').rlike('๐คง'),"๐คง").when(col('text').rlike('๐'),"๐").when(col('text').rlike('๐ฅต'),"๐ฅต").when(col('text').rlike('๐'),"๐").when(col('text').rlike('๐คฃ'),"๐คฃ").when(col('text').rlike('๐ฅฐ'),"๐ฅฐ").when(col('text').rlike('๐ค'),"๐ค").otherwise('Emoji nรฃo encontrado'))
lista_negativos = '๐ต|๐ฅด|๐ฒ|๐คฏ|๐ค|๐ท|๐ค|๐ค|๐คฎ|๐คข|๐คง|๐ฅต|๐ฅถ|๐ด|๐|๐ฟ|๐น|๐บ|๐ฉ|๐ฎ|๐ฑ|๐จ|๐ฐ|๐ฏ|๐ฆ|๐ง|๐ข|๐ฅ|๐ช|๐คค|๐|๐ญ|๐|๐ |๐ก|๐คฌ|๐|๐|๐|๐ฌ|๐ฅบ|๐ฃ|๐|๐ซ|๐ฉ|๐ฅฑ|๐ค๐ถ|๐|๐|๐|๐|๐คจ|๐ค|๐คซ|๐|๐คฅ'
lista_positivos = '๐|๐|๐|๐|๐|๐|๐|๐คฃ|๐|๐|๐|๐|๐|๐|๐|๐|๐ฅฐ|๐|๐|๐|๐|๐คช|๐|๐|๐|๐ค|๐|๐ค|๐ง|๐ค |๐ฅณ|๐ค|๐คก|๐|๐คญ|๐ณ|๐คฉ'

df = df['created_at','id', 'text', 'Sentimento', 'Emoji']
display(df)
df.write.parquet("wasbs://yuri-refdata@otaviodata.blob.core.windows.net/twitter3/tweets_analisados_spark_emojis_wpp.parquet")