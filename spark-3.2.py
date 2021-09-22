import os
from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession

caminho = os.getcwd()

spark = SparkSession \
    .builder \
    .appName("Exercícios") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

df_nomes = spark.read.csv(caminho + '/nomes.csv', header=True)

df_nomes.createOrReplaceTempView('pessoas')

spark.sql('SELECT nome, total, ano, ranking FROM(SELECT nome, total, ano, '
          'RANK() OVER(ORDER BY cast(total as INT) DESC) AS ranking FROM pessoas)').show()
