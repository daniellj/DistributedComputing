## Apache Spark
### 1. Introdutction
Highly scalable distributed data analysis system based on the shared use of the RAM memory of the nodes of a cluster as a processing source (in this mode, 100x faster than Hadoop MapReduce). Apache Spark also allows disk processing (in this mode, 10X faster than Hadoop MapReduce) in case there is not enough RAM memory in the cluster nodes to process the required amount of data.

Comparative table Hadoop Map Reduce X Apache Spark:

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
                     <td>Batch processing</td>
                     <td>Batch, iterative or continuous processing (data streaming)</td>
                  </tr>
                  <tr>
                     <td>MapReduce for analytical processing, consisting of disk data/td>
                     <td>Performs all necessary analytical operations in memory</td>
                  </tr>
                  <tr>
                     <td>Stores intermediate processing results in DISK</td>
                     <td>Stores intermediate processing results in RAM Memory</td>
                  </tr>
                  <tr>
                     <td>HA: data are written to disk after each operation (mapping, reduction, grouping, ...), distributed throughout the data cluster</td>
                     <td>HA: create virtual data objects that are stored in resilient datasets, distributed throughout the data cluster</td>
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