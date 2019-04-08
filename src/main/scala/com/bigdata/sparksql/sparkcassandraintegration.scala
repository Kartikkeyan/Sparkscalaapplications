package com.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._

object sparkcassandraintegration {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("sparkcassandraintegration").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("sparkcassandraintegration").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("sparkcassandraintegration").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val sparkcsdf = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "asl", "keyspace" -> "cassandradb")).load() // This Dataset will use a spark.cassandra.input.size of 128
   // sparkcsdf.show(truncate = false)

    //reading data from file in local
    val casdata = "file:///E:\\Kartik\\Work\\datasets\\cassandrasample.txt"
    val casdf = spark.read.format("csv").option("header","true").option("inferSchema","true").load(casdata)
    //writing data to cassandra
    casdf.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").option("table","asl").option("keyspace","cassandradb").save()
    casdf.show()

    spark.stop()
  }
}

