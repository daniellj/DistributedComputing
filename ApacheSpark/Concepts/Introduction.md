## Apache Spark
#### 1. Introdutction
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
	* 2.1. SparkSQL
	* 2.2. Spark Streaming: in this component, Spark receive DATA INGESTION through a variety of sources, and could process this data with complex algorithms and/or high-level functions (map, reduction, join and window). Internally, receives the data in real time and splits it into batches, where these are processed by the Spark Engine, generating the output of this processing , that may be a disk recording on file systems (HDFS), write records to the database, or real-time dashboards.
![img](https://github.com/daniellj/DistributedComputing/blob/master/ApacheSpark/Concepts/img/apache-spark-streaming-arch.png)

It also provides a high level abstraction called discrete flow (DStream), which represents a continuous flow of data.
![img](https://github.com/daniellj/DistributedComputing/blob/master/ApacheSpark/Concepts/img/apache-spark-streaming-flow.png)
	* 2.3. MLlib
	* 2.4. GraphX
	* 2.5. Spark Core
	* 2.6. Spark Standalone
	* 2.7. Hadoop Yarn
	* 2.8. Mesos

#### 3. Apache Spark Architecture
The following components are part of the architecture:
	* 3.1. SparkContext
Context represents the CONNECTION to a Spark cluster, and can be used to create RDDs, accumulators and broadcast variables on that cluster. For default, only one SparkContext may be active per JVM, but can be removed.

![img](https://github.com/daniellj/DistributedComputing/blob/master/ApacheSpark/Concepts/img/apache-spark-arch.png)

	* 3.2. Cluster Manager
	* 3.3. Workers Node
	* 3.3.1. Executor
	* 3.3.1. Cache
	* 3.3.1. Task

#### 4. Data Sources and Formats to Consume
![img](https://github.com/daniellj/DistributedComputing/blob/master/ApacheSpark/Concepts/img/apache-spark-datasource.png)

#### 5. Deploy Mode
	* Local (standalone or cluster)
	* Clustering in the Cloud (Databricks, Amazon EC2, IBM Bluemix)

#### 6. RDD's - Resilient Distributed Datasets
Is an immutable distributed collection of objects (read-only). Each RDD is split into multiple partitions, which may be computed on different nodes of the cluster (distributed data processing). Type of objects that RDD may contain: Scala, Python, Java or user-defined classes.
-> Resilient -> fault-tolerant: recompute missing or damaged partitions due to node failures.
-> Distributed -> data residing on multiple nodes in a cluster.
-> Dataset -> is a collection of partitioned data with primitive values.
![img](https://github.com/daniellj/DistributedComputing/blob/master/ApacheSpark/Concepts/img/apache-spark-RDD.png)

	* 6.1. There are two ways to create RDDs:
	* Parallelizing an existing collection in your driver program.
	* Referencing a external dataset (HDFS, RDBMS, NoSQL, S3)

	* 6.2. Support two kinds of operations:
	* TRANSFORMATIONS - create a new dataset from an existing one (lazy operations that return another RDD). Some of the common TRANSFORMATIONS supported:

<table class="table">
<tr><th style="width:25%">Transformation</th><th>Meaning</th></tr>
<tr>
  <td> <b>map</b>(<i>func</i>) </td>
  <td> Return a new distributed dataset formed by passing each element of the source through a function <i>func</i>. </td>
</tr>
<tr>
  <td> <b>filter</b>(<i>func</i>) </td>
  <td> Return a new dataset formed by selecting those elements of the source on which <i>func</i> returns true. </td>
</tr>
<tr>
  <td> <b>flatMap</b>(<i>func</i>) </td>
  <td> Similar to map, but each input item can be mapped to 0 or more output items (so <i>func</i> should return a Seq rather than a single item). </td>
</tr>
<tr>
  <td> <b>mapPartitions</b>(<i>func</i>) <a name="MapPartLink"></a> </td>
  <td> Similar to map, but runs separately on each partition (block) of the RDD, so <i>func</i> must be of type
    Iterator&lt;T&gt; =&gt; Iterator&lt;U&gt; when running on an RDD of type T. </td>
</tr>
<tr>
  <td> <b>mapPartitionsWithIndex</b>(<i>func</i>) </td>
  <td> Similar to mapPartitions, but also provides <i>func</i> with an integer value representing the index of
  the partition, so <i>func</i> must be of type (Int, Iterator&lt;T&gt;) =&gt; Iterator&lt;U&gt; when running on an RDD of type T.
  </td>
</tr>
<tr>
  <td> <b>sample</b>(<i>withReplacement</i>, <i>fraction</i>, <i>seed</i>) </td>
  <td> Sample a fraction <i>fraction</i> of the data, with or without replacement, using a given random number generator seed. </td>
</tr>
<tr>
  <td> <b>union</b>(<i>otherDataset</i>) </td>
  <td> Return a new dataset that contains the union of the elements in the source dataset and the argument. </td>
</tr>
<tr>
  <td> <b>intersection</b>(<i>otherDataset</i>) </td>
  <td> Return a new RDD that contains the intersection of elements in the source dataset and the argument. </td>
</tr>
<tr>
  <td> <b>distinct</b>([<i>numTasks</i>])) </td>
  <td> Return a new dataset that contains the distinct elements of the source dataset.</td>
</tr>
<tr>
  <td> <b>groupByKey</b>([<i>numTasks</i>]) <a name="GroupByLink"></a> </td>
  <td> When called on a dataset of (K, V) pairs, returns a dataset of (K, Iterable&lt;V&gt;) pairs. <br />
    <b>Note:</b> If you are grouping in order to perform an aggregation (such as a sum or
      average) over each key, using <code>reduceByKey</code> or <code>aggregateByKey</code> will yield much better
      performance.
    <br />
    <b>Note:</b> By default, the level of parallelism in the output depends on the number of partitions of the parent RDD.
      You can pass an optional <code>numTasks</code> argument to set a different number of tasks.
  </td>
</tr>
<tr>
  <td> <b>reduceByKey</b>(<i>func</i>, [<i>numTasks</i>]) <a name="ReduceByLink"></a> </td>
  <td> When called on a dataset of (K, V) pairs, returns a dataset of (K, V) pairs where the values for each key are aggregated using the given reduce function <i>func</i>, which must be of type (V,V) =&gt; V. Like in <code>groupByKey</code>, the number of reduce tasks is configurable through an optional second argument. </td>
</tr>
<tr>
  <td> <b>aggregateByKey</b>(<i>zeroValue</i>)(<i>seqOp</i>, <i>combOp</i>, [<i>numTasks</i>]) <a name="AggregateByLink"></a> </td>
  <td> When called on a dataset of (K, V) pairs, returns a dataset of (K, U) pairs where the values for each key are aggregated using the given combine functions and a neutral "zero" value. Allows an aggregated value type that is different than the input value type, while avoiding unnecessary allocations. Like in <code>groupByKey</code>, the number of reduce tasks is configurable through an optional second argument. </td>
</tr>
<tr>
  <td> <b>sortByKey</b>([<i>ascending</i>], [<i>numTasks</i>]) <a name="SortByLink"></a> </td>
  <td> When called on a dataset of (K, V) pairs where K implements Ordered, returns a dataset of (K, V) pairs sorted by keys in ascending or descending order, as specified in the boolean <code>ascending</code> argument.</td>
</tr>
<tr>
  <td> <b>join</b>(<i>otherDataset</i>, [<i>numTasks</i>]) <a name="JoinLink"></a> </td>
  <td> When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (V, W)) pairs with all pairs of elements for each key.
    Outer joins are supported through <code>leftOuterJoin</code>, <code>rightOuterJoin</code>, and <code>fullOuterJoin</code>.
  </td>
</tr>
<tr>
  <td> <b>cogroup</b>(<i>otherDataset</i>, [<i>numTasks</i>]) <a name="CogroupLink"></a> </td>
  <td> When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (Iterable&lt;V&gt;, Iterable&lt;W&gt;)) tuples. This operation is also called <code>groupWith</code>. </td>
</tr>
<tr>
  <td> <b>cartesian</b>(<i>otherDataset</i>) </td>
  <td> When called on datasets of types T and U, returns a dataset of (T, U) pairs (all pairs of elements). </td>
</tr>
<tr>
  <td> <b>pipe</b>(<i>command</i>, <i>[envVars]</i>) </td>
  <td> Pipe each partition of the RDD through a shell command, e.g. a Perl or bash script. RDD elements are written to the
    process's stdin and lines output to its stdout are returned as an RDD of strings. </td>
</tr>
<tr>
  <td> <b>coalesce</b>(<i>numPartitions</i>) <a name="CoalesceLink"></a> </td>
  <td> Decrease the number of partitions in the RDD to numPartitions. Useful for running operations more efficiently
    after filtering down a large dataset. </td>
</tr>
<tr>
  <td> <b>repartition</b>(<i>numPartitions</i>) </td>
  <td> Reshuffle the data in the RDD randomly to create either more or fewer partitions and balance it across them.
    This always shuffles all data over the network. <a name="RepartitionLink"></a></td>
</tr>
<tr>
  <td> <b>repartitionAndSortWithinPartitions</b>(<i>partitioner</i>) <a name="Repartition2Link"></a></td>
  <td> Repartition the RDD according to the given partitioner and, within each resulting partition,
  sort records by their keys. This is more efficient than calling <code>repartition</code> and then sorting within
  each partition because it can push the sorting down into the shuffle machinery. </td>
</tr>
</table>

	* ACTIONS - return a value to the driver program after running a computation on the dataset (map, reduce, etc). Some of the common ACTIONS supported:

<table class="table">
<tr><th>Action</th><th>Meaning</th></tr>
<tr>
  <td> <b>reduce</b>(<i>func</i>) </td>
  <td> Aggregate the elements of the dataset using a function <i>func</i> (which takes two arguments and returns one). The function should be commutative and associative so that it can be computed correctly in parallel. </td>
</tr>
<tr>
  <td> <b>collect</b>() </td>
  <td> Return all the elements of the dataset as an array at the driver program. This is usually useful after a filter or other operation that returns a sufficiently small subset of the data. </td>
</tr>
<tr>
  <td> <b>count</b>() </td>
  <td> Return the number of elements in the dataset. </td>
</tr>
<tr>
  <td> <b>first</b>() </td>
  <td> Return the first element of the dataset (similar to take(1)). </td>
</tr>
<tr>
  <td> <b>take</b>(<i>n</i>) </td>
  <td> Return an array with the first <i>n</i> elements of the dataset. </td>
</tr>
<tr>
  <td> <b>takeSample</b>(<i>withReplacement</i>, <i>num</i>, [<i>seed</i>]) </td>
  <td> Return an array with a random sample of <i>num</i> elements of the dataset, with or without replacement, optionally pre-specifying a random number generator seed.</td>
</tr>
<tr>
  <td> <b>takeOrdered</b>(<i>n</i>, <i>[ordering]</i>) </td>
  <td> Return the first <i>n</i> elements of the RDD using either their natural order or a custom comparator. </td>
</tr>
<tr>
  <td> <b>saveAsTextFile</b>(<i>path</i>) </td>
  <td> Write the elements of the dataset as a text file (or set of text files) in a given directory in the local filesystem, HDFS or any other Hadoop-supported file system. Spark will call toString on each element to convert it to a line of text in the file. </td>
</tr>
<tr>
  <td> <b>saveAsSequenceFile</b>(<i>path</i>) <br /> (Java and Scala) </td>
  <td> Write the elements of the dataset as a Hadoop SequenceFile in a given path in the local filesystem, HDFS or any other Hadoop-supported file system. This is available on RDDs of key-value pairs that implement Hadoop's Writable interface. In Scala, it is also
   available on types that are implicitly convertible to Writable (Spark includes conversions for basic types like Int, Double, String, etc). </td>
</tr>
<tr>
  <td> <b>saveAsObjectFile</b>(<i>path</i>) <br /> (Java and Scala) </td>
  <td> Write the elements of the dataset in a simple format using Java serialization, which can then be loaded using
    <code>SparkContext.objectFile()</code>. </td>
</tr>
<tr>
  <td> <b>countByKey</b>() <a name="CountByLink"></a> </td>
  <td> Only available on RDDs of type (K, V). Returns a hashmap of (K, Int) pairs with the count of each key. </td>
</tr>
<tr>
  <td> <b>foreach</b>(<i>func</i>) </td>
  <td> Run a function <i>func</i> on each element of the dataset. This is usually done for side effects such as updating an <a href="#accumulators">Accumulator</a> or interacting with external storage systems.
  <br /><b>Note</b>: modifying variables other than Accumulators outside of the <code>foreach()</code> may result in undefined behavior. See <a href="#understanding-closures-a-nameclosureslinka">Understanding closures </a> for more details.</td>
</tr>
</table>


Credits:

https://spark.apache.org

https://training.databricks.com/visualapi.pdf

https://mapr.com/ebooks/spark/apache-spark-cheat-sheet.html

https://github.com/rklick-solutions/spark-tutorial/wiki/Spark-Core#introduction-to-apache-spark

https://jaceklaskowski.gitbooks.io/mastering-apache-spark/