from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext('local[2]', 'ContarPalavrasStreaming')
ssc = StreamingContext(sc, 10)
ssc.checkpoint('checkpoint')

def funcaoAtualizar(novos_valores, ultimos_valores):
    return sum(novos_valores) + (ultimos_valores or 0)

dados = ssc.socketTextStream('localhost', 3456)

#Calcula a contagem
palavras = dados.flatMap(lambda linha: linha.split(" "))
pares = palavras.map(lambda palavra: (palavra, 1))
contagem = pares.updateStateByKey(funcaoAtualizar)
contagem.pprint()
# ssc.start()
# ssc.awaitTermination()