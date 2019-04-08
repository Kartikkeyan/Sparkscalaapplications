package com.bigdata.sparkstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object sparkkafkaproducerprac1 {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("sparkkafkaproducerprac1").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("sparkkafkaproducerprac1").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("sparkkafkaproducerprac1").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val data = "file:///E:\\Kartik\\Work\\nifinew"

    spark.stop()
  }
}

