#2
from pyspark.sql import SparkSession
import pandas as pd

lista_animais = sorted(['Jacare','Hipopotamo','Ornitorrinco','Zebra','Leao','Chimpanze','Girafa','Pinguim','Macaco','Crocodilo','Abutre','Abelha','Elefante','Hiena','Suricato','Javali','Veado','Gaviao','Cachorro','Gato'])
df = pd.DataFrame({'col':lista_animais})

df.to_csv('C:\\Users\\yuria\\Desktop\\a\\Compasso UOL\\Semana 9\\Exercícios\\csvs\\animais.csv')
