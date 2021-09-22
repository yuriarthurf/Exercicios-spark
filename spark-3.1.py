#1
import os
from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession

caminho = os.getcwd()

spark = SparkSession \
    .builder \
    .appName("Exercícios") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')
sqlContext = SQLContext(spark.sparkContext)

df_nomes = spark.read.csv(caminho + '/nomes.csv', header=True)
df_nomes.createOrReplaceTempView('pessoas')

#R.1
spark.sql('SELECT sexo, count(sexo) as contador FROM pessoas GROUP BY sexo')

#R.2
feminino = df_nomes.filter(df_nomes.sexo == 'F').select(df_nomes.sexo).count()
masculino = df_nomes.filter(df_nomes.sexo == 'M').select(df_nomes.sexo).count()
print(feminino - masculino)

#R.1 - O sexo feminino é o mais recorrente (1081683)
#R.2 - A diferença é de 337933
