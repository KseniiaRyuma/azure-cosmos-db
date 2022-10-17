// Databricks notebook source
//Spark connector
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

// COMMAND ----------

spark.conf.set("spark.cassandra.connection.host", "astrademo.cassandra.cosmos.azure.com")
spark.conf.set("spark.cassandra.connection.port", "10350")
spark.conf.set("spark.cassandra.connection.ssl.enabled", true)
spark.conf.set("spark.cassandra.auth.username", "astrademo")
spark.conf.set("spark.cassandra.auth.password", "8mT3NJaFBRRNkbB1dqteKVZ....")

//Throughput-related...adjust as needed
spark.conf.set("spark.cassandra.output.batch.size.rows", 1)

spark.conf.set("spark.cassandra.connection.remoteConnectionsPerExecutor", 10) // Spark 3.x
spark.conf.set("spark.cassandra.output.concurrent.writes", 1000)
spark.conf.set("spark.cassandra.concurrent.reads", 512)
spark.conf.set("spark.cassandra.output.batch.grouping.buffer.size", 1000)
spark.conf.set("spark.cassandra.connection.keep_alive_ms", 600000000)


// COMMAND ----------

val readBooksDF = spark.read.cassandraFormat("user", "uprofile", "").load()

// COMMAND ----------

readBooksDF.take(7)

// COMMAND ----------


//Write DataFrame data to parquet file
readBooksDF.write.parquet("/tmp/spark_output/data_parquet")



// COMMAND ----------

val df = spark.read.csv("/tmp/spark_output/datacsv1")
df.take(10)

// COMMAND ----------


