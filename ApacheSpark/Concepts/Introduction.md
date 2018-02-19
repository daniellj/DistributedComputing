## Apache Spark
### 1. Introdutction
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

#### 2. Apache Spark Platform
The following components are part of the platform:
![img](https://github.com/daniellj/DistributedComputing/blob/master/ApacheSpark/Concepts/img/apache-spark-platform.png)
- 2.1. SparkSQL
- 2.2. Spark Streaming: in this component, Spark receive DATA INGESTION through a variety of sources, and could process this data with complex algorithms and/or high-level functions (map, reduction, join and window). Internally, receives the data in real time and splits it into batches, where these are processed by the Spark Engine, generating the output of this processing , that may be a disk recording on file systems (HDFS), write records to the database, or real-time dashboards.
![img](https://github.com/daniellj/DistributedComputing/blob/master/ApacheSpark/Concepts/img/apache-spark-streaming-arch.png)

It also provides a high level abstraction called discrete flow (DStream), which represents a continuous flow of data.
![img](https://github.com/daniellj/DistributedComputing/blob/master/ApacheSpark/Concepts/img/apache-spark-streaming-flow.png)
- 2.3. MLlib
- 2.4. GraphX
- 2.5. Spark Core
- 2.6. Spark Standalone
- 2.7. Hadoop Yarn
- 2.8. Mesos

#### 3. Apache Spark Architecture
The following components are part of the architecture:
- 3.1. SparkContext
Context represents the CONNECTION to a Spark cluster, and can be used to create RDDs, accumulators and broadcast variables on that cluster. For default, only one SparkContext may be active per JVM, but can be removed.

![img](https://github.com/daniellj/DistributedComputing/blob/master/ApacheSpark/Concepts/img/apache-spark-arch.png)

- 3.2. Cluster Manager
- 3.3. Workers Node
- 3.3.1. Executor
- 3.3.1. Cache
- 3.3.1. Task

#### 4. RDD's - Resilient Distributed Datasets
Is an immutable distributed collection of objects (read-only). Each RDD is split into multiple partitions, which may be computed on different nodes of the cluster (distributed data processing). Type of objects that RDD may contain: Scala, Python, Java or user-defined classes.
- 4.1. Resilient -> fault-tolerant: recompute missing or damaged partitions due to node failures.
- 4.2. Distributed -> data residing on multiple nodes in a cluster.
- 4.3. Dataset -> is a collection of partitioned data with primitive values.
![img](https://github.com/daniellj/DistributedComputing/blob/master/ApacheSpark/Concepts/img/apache-spark-RDD.png)
	
RDDs support two kinds of operations:
- Transformations - lazy operations that return another RDD.
- Actions - operations that trigger computation and return values.

Credits:

https://spark.apache.org

https://github.com/rklick-solutions/spark-tutorial/wiki/Spark-Core#introduction-to-apache-spark

https://jaceklaskowski.gitbooks.io/mastering-apache-spark/