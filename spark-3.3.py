import os
from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

caminho = os.getcwd()

spark = SparkSession \
    .builder \
    .appName("Exercícios") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

df_nomes = spark.read.csv(caminho + '/nomes.csv', header=True)

df_select = df_nomes.select(df_nomes.nome, df_nomes.ano, (df_nomes.ano.between(1980,1989)).alias('DECADA_80'),
                            (df_nomes.ano.between(1990,1999)).alias('DECADA_90'),
                            (df_nomes.ano.between(2000,2009)).alias('DECADA_00'))
df_select = df_select.withColumn('DECADA_80', F.when(F.col('DECADA_80'), F.lit('Sim')).otherwise('Não')).withColumn('DECADA_90', F.when(F.col('DECADA_90'), F.lit('Sim')).otherwise('Não')).withColumn('DECADA_00', F.when(F.col('DECADA_00'), F.lit('Sim')).otherwise('Não'))

df_select.show()
