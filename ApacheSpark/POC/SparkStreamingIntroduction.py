
# coding: utf-8

# # Lab 1
# ### Simulando a coleta de dados com Spark Streaming através da "escuta" em uma porta TCP-IP especificada
# #### Para isso, iremos usar o netcat como ferramenta de apoio.
# porta escolhida: 22121
# 
# abrir o terminal do linux e digitar o comando abaixo:
# 
# nc -lk 22121

# ## 1. Importando os módulos necessários para o Streaming de Dados

# In[1]:


#fonte: https://github.com/danielsan/Spark-Streaming-Examples/blob/master/spark-streaming-foreachRDD-and-foreach.py
# Módulos do Spark

from pyspark.streaming import StreamingContext
from pyspark import SparkContext # quando usamos o PYSPARK, o SPARK CONTEXT já é criado por default: sc


# ## 2. Criando o contexto com o Spark Streaming
# - Lembrando que o contexto com a aplicação Spark, por default pelo PYSPARK já é criado automaticamente com o nome "sc"

# In[2]:


print('-->> Verificando o contexto em que se encontra a conexão:', sc) #sc = spark context
print('-->> Versão do SPARK em execução:', sc.version)

# Definindo o contexto do Streaming de dados com Spark, uma vez que o contexto com o Spark já foi criado por default
strcontext = StreamingContext(sparkContext = sc, batchDuration = 1)


# ## 3. Criando o RECEIVER do Spark.
# - No caso, estamos usando o socketTextStream por se tratar de uma conexão à uma porta TCP-IP
# - A coleta de dados é possível através do Twitter, Apache Flume, Apache Kafka, HDFS do Hadoop, IOT: ou seja, as fontes de dados para o RECEIVER que irá "alimentar" o Streaming do Spark. Veja, são inúmeras.

# In[3]:


# Criando o RECEIVER para fazer o streaming de dados TCP-IP = socketTextStream
hostname = "localhost"
port = 22121

lines = strcontext.socketTextStream(hostname = hostname, port = port)
print("Type object 'lines':", lines)


# ## 4. Tratamento e Tranformação
# ### 4.1. Para cada linha, divide as palavras a cada " " (espaço) encontrado

# In[4]:


# como estamos executando função de transforamção sobre o DSTREAM gerado (lines), então devemos "jogar" o resultado
# da transformação em um novo DSTREAM, pois este é sempre IMUTÁVEL.
words = lines.flatMap(lambda lines : lines.split(" "))

print("Type object 'words':", words)


# ### 4.2. Conta o número de ocorrências das palavras em cada batch entregue pelo streaming de dados

# In[5]:


pairs = words.map(lambda words : (words, 1))
# Exemplo de saída: (('ciência', 1), ('Big Data', 2), ('abacaxi', 1))

wordCounts = pairs.reduceByKey(lambda x, y: x + y) # onde a chave é a própria palavra!
print("Type object 'wordCounts':", type(wordCounts))


# ## 5. Imprimindo os primeiros elementos de cada RDD gerado no DStream
# RDD = Resilient Distributed Dataset

# In[6]:


wordCounts.pprint()


# ## 6. Início e encerramento da coleta do stream de dados
# 
# - strcontext.start() = Iniciando a coleta e processamento do stream de dados.
# - strcontext.awaitTermination() = a coleta de dados por streaming irá rodar indefinidamente até que encontre um erro de execução ou caso finalize todo o trabalho de streaming de dados.

# In[ ]:


strcontext.start()
strcontext.awaitTermination()


# # Lab 2
# ## 1. Usando as técnicas de Windowing
# 
# Imagem: https://prateekvjoshi.files.wordpress.com/2015/11/2-windowed-processing.png
# 
# Window Operations: https://spark.apache.org/docs/latest/streaming-programming-guide.html#window-operations

# In[ ]:


# Módulos do Spark

from pyspark import SparkContext # quando usamos o PYSPARK, o SPARK CONTEXT já é criado por default: sc
from pyspark.streaming import StreamingContext

print('-->> Verificando o contexto em que se encontra a conexão:', sc) #sc = spark context
print('-->> Versão do SPARK em execução:', sc.version)

def split_words(line):
    '''
    Input: a linha inteira capturada pelo streaming.
    Output: após a captura da linha, faz a quebra após encontrar um " " (espaços em branco).
            Retorna um dicionário (chave, valor).
    '''
    try:
        if (bool(line.strip()) == True): #verifica se o valor de entrada é:
            output = line.strip(" ")     #um conteúdo não NULO
        return(output)
    except:
        output = ''
        return(output)

if __name__ == "__main__":
    # Definindo o valor de captura do DSTREAM
    batchDuration = 1 # tempo em segundos
    
    # Definindo valores para os parâmetros da WINDOWED
    windowLength = 15 * batchDuration # se intervalo do lote = 1 seg, então o tamanho da janela será de 15 RDD's.
    slideInterval = 5 * batchDuration  # slideInterval = no momento que ocorrer o evento da window,a partir de que
                                 # conjunto de dados deve-se considerar para as trabalhar as transformações e ações
                                 # se intervalo do lote = 1 seg, o tamanho da janela = 15, sliding = 5, então
                                 # iremos considerar 10 RDD's de um total de 15.

    # Definindo o SparkContext
    spark_context = sc

    # Definindo o contexto do Streaming de dados com Spark, uma vez que o contexto com o Spark já foi criado por default
    streaming_context = StreamingContext(sparkContext = spark_context, batchDuration = batchDuration)
    streaming_context.checkpoint("checkpoint")

    hostname = "localhost"
    port = 22121

    #-->> Chamando a função para construção do RECEIVER
    lines = streaming_context.socketTextStream(hostname = hostname, port = port)

    #-->> Caso queira chamar via linha de comando e passando os argumentos, considerar comando abaixo:
    #lines = streaming_context.socketTextStream(hostname = sys.argv[1], port = int(sys.argv[2]))

    #-->> Para cada linha, divide as palavras a cada " " (espaço) encontrado
    words = lines.flatMap(lambda line : line.split(" "))
    #-->> 'words' seria algo como = "abacaxi abacaxi Data " => [('abacaxi', 'Data', 'abacaxi', '')]

    #-->> Conta o número de ocorrências das palavras em cada batch entregue pelo streaming de dados
    pairs = words.map(lambda words : (words, 1))
    #-->> 'pairs' seria algo como: [('abacaxi', 1), ('abacaxi', 1), ('Data', 1), ('abacaxi', 1), ('', 1)]
    
    wordCounts = pairs.reduceByKeyAndWindow((lambda x, y: x + y), (lambda x, y: x - y), windowLength, slideInterval) # onde a chave é a própria palavra!
    #-->> 'wordsCount' seria algo como: [('abacaxi', 3), ('Data', 1), ('', 1)]
    
    wordCounts.pprint()
    
    streaming_context.start()
    streaming_context.awaitTermination()


# In[ ]:


'''
Linhas para o streaming coletar:

x x x x x x
x x x x x x

c c c c c c
c c c c c c

abacaxi abacaxi abacaxi abacaxi abacaxi cera cera cera cera borracha borracha borracha borracha
abacaxi abacaxi abacaxi abacaxi abacaxi cera cera cera cera borracha borracha borracha borracha
abacaxi abacaxi abacaxi abacaxi abacaxi cera cera cera cera borracha borracha borracha borracha
abacaxi abacaxi abacaxi abacaxi abacaxi cera cera cera cera borracha borracha borracha borracha
abacaxi abacaxi abacaxi abacaxi abacaxi cera cera cera cera borracha borracha borracha borracha
abacaxi abacaxi abacaxi abacaxi abacaxi cera cera cera cera borracha borracha borracha borracha
xis xis xis morango morango morango morango morango morango morango morango morango morango morango
xis xis xis morango morango morango morango morango morango morango morango morango morango morango
xis xis xis morango morango morango
xis xis xis morango morango morango morango morango morango morango morango morango morango morango
xis xis xis morango morango morango morango morango morango morango morango morango morango morango
xis xis xis morango morango morango morango morango
moleton moleton moleton camisa camisa camisa
teste teste Data Science Data Science Data Science Data Science Data Science
teste teste 
Data Science
'''


# ### 2. Encerrando o RECEIVER...

# In[ ]:


strcontext.stop()

