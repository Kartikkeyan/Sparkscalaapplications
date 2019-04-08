package com.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object sparkhiveintegration {
  def main(args: Array[String]) {
    //to process hive data in spark add enableHiveSupport() in spark section
    //val spark = SparkSession.builder.master("local[*]").appName("sparkhiveintegration").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("sparkhiveintegration").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("sparkhiveintegration").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql

    spark.stop()
  }
}

