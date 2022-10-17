// Databricks notebook source
import org.apache.spark.sql.functions._
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import spark.implicits._

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
 
val dbName = "CosmosDBtoAstra"
val keyspace = "uprofile"

 
spark.conf.set(s"spark.sql.catalog.$dbName", "com.datastax.spark.connector.datasource.CassandraCatalog")
spark.sql(s"use $dbName.$keyspace")
spark.sql("show tables").show()

// COMMAND ----------

val df = spark.read.parquet("/tmp/spark_output/data_parquet")
df.take(10)

// COMMAND ----------

df.write.mode("append").cassandraFormat("user", "uprofile", "").save()
