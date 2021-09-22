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

df_select = df_nomes.select(df_nomes.nome, df_nomes.ano, (df_nomes.ano <= 1989).alias('DECADA_80'),
                            (df_nomes.ano <= 1999).alias('DECADA_90'),
                            (df_nomes.ano <= 2009).alias('DECADA_00'))


dropa_coluna = df_select.drop('DECADA_00')
dropa_coluna.show()
