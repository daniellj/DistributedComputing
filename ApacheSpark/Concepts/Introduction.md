## Apache Spark
### 1. Introdução
Definição: sistema para análise de dados com processamento distribuído, com alta escalabilidade, se apoiando principalmente do uso compartilhado da memória RAM dos nodos de um cluster como fonte de processamento (nesse modo, 100X + rápido que o Hadoop MapReduce). Lembrando que Apache Spark também permite processamento em disco (nesse modo, 10X + rápido que o Hadoop MapReduce), no caso de não haver memória RAM suficiente nos nodos do cluster para processar a quantidade necessária de dados.

Principais Diferenças:

<html>
   <body>
      <table border = "1" width = "100%">         
         <tr>
            <td>
               <table border = "1" width = "100%">
                  <tr>
                     <th>Hadoop Map Reduce</th>
                     <th>Apache Spark</th>
                  </tr>
                  <tr>
                     <td>Processamento em lote</td>
                     <td>Processamento em lote, iterativo ou em processamento contínuo (streaming de dados)</td>
                  </tr>
                  <tr>
                     <td>Utiliza o MapReduce para procesamento analítico, consistindo os dados em disco</td>
                     <td>Realiza todas as operações analíticas necessárias em memória</td>
                  </tr>
                  <tr>
                     <td>Armazena os resultados intermediários do processamento em DISCO</td>
                     <td>Armazena os resultados intermediários do processamento em MEMÓRIA</td>
                  </tr>
                  <tr>
                     <td>HA: dados são gravados em disco depois de cada operação (mapeamento, redução, agrupamento, ...), distribuídos por todo o “cluster” de dados</td>
                     <td>HA: cria objetos virtuais de dados que são armazenados em conjuntos de dados resilientes, distribuídos por todo o “cluster” de dados</td>
                  </tr>
               </table>
            </td>
         </tr>
      </table>
   </body>
</html>

Além do processamento em memória, outra grande feature do Spark que podemos abordar, é o processamento de dados em tempo real.

Nesse cenário, Spark receberia INGESTÃO DE DADOS através de diversas origens, podendo processar esses dados com algoritmos complexos e/ou funções de alto nível (map, reduce, join and window). A saída deste processamento pode ser a escrita em disco em sistemas de arquivos (HDFS) ou em bancos de dados, ou ainda dashboards em tempo real.

Segue arquitetura arquitetura do Apache Spark atuando em Streaming de dados:

#### 1.1. Apache Spark Architecture
Internamente, Spark Streaming recebe os dados em tempo real e divide-os em lotes, onde estes são processados pela engine do Spark, gerando o resultado final.
![img](https://github.com/daniellj/DistributedComputing/blob/master/ApacheSpark/Concepts/img/apache-spark-streaming-arch.png)

#### 1.2. Apache Spark Flow
Spark Streaming fornece uma abstração de alto nível chamada fluxo discretizado (DStream), que representa um fluxo contínuo de dados.
![img](https://github.com/daniellj/DistributedComputing/blob/master/ApacheSpark/Concepts/img/apache-spark-streaming-flow.png)

#### 1.3. RDD's - Resilient Distributed Datasets
São coleções de objetos IMUTÁVEIS.

#### 1.4. SparkContext


#### 1.5. RDD transformations


Créditos: https://spark.apache.org/docs/latest/streaming-programming-guide.html
