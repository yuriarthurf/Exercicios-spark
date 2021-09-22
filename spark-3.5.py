import os
from pyspark.sql import SparkSession
import pandas as pd
import pyspark

caminho = os.getcwd()

spark = SparkSession \
    .builder \
    .appName("Exercícios") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

df_nomes = spark.read.csv(caminho + '/nomes.csv', header=True)
df_nomes.createOrReplaceTempView('pessoas')

df_select = spark.sql('SELECT nome, ano, sexo FROM pessoas WHERE (ano BETWEEN 1980 and 1989)')
df_select2 = df_select.filter(df_select.sexo == 'F').select(df_select.nome, df_select.ano, df_select.sexo)

df_select3 = spark.sql('SELECT nome, ano, sexo FROM pessoas WHERE (ano BETWEEN 1980 and 1989)')
df_select4 = df_select.filter(df_select.sexo == 'M').select(df_select.nome, df_select.ano, df_select.sexo)

pandasDF = df_select2.toPandas()
pandasDF2 = df_select4.toPandas()

df = pd.DataFrame(pandasDF, columns=['nome', 'ano', 'sexo'])
df2 = pd.DataFrame(pandasDF2, columns=['nome', 'ano', 'sexo'])

df.to_csv('C:\\Users\\yuria\\Desktop\\a\\Compasso UOL\\Semana 9\\Exercícios\\csvs\\mulheres.csv', index = False)
df2.to_csv('C:\\Users\\yuria\\Desktop\\a\\Compasso UOL\\Semana 9\\Exercícios\\csvs\\homens.csv', index = False)
