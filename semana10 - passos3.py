from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
sc = SparkContext()
sc.setLogLevel('ERROR')

spark = SparkSession.builder.appName('ContarPalavrasEstruturado').getOrCreate()
linhas = spark.readStream.format('socket').option('host', 'localhost').option('port', 3456).load()

palavras = linhas.select(explode(split(linhas.value, ' ')).alias('palavra'))
contagem = palavras.groupBy('palavra').count()
consulta = contagem.writeStream.outputMode('complete').format('console').start()
consulta.awaitTermination()