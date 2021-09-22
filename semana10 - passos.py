from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel

sc = SparkContext('local[2]', 'ContarPalavrasStreaming')
ssc = StreamingContext(sc, 10)
dados = ssc.socketTextStream('localhost', 3456, storageLevel=StorageLevel(True, True, False, False, 1))

palavras = dados.flatMap(lambda linha: linha.split(" "))

pares = palavras.map(lambda palavra: (palavra, 1))

contagem = pares.reduceByKey(lambda x, y: x + y)
contagem.pprint()

ssc.start()
ssc.awaitTermination()
