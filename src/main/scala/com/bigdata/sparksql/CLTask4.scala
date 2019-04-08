package com.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object CLTask4 {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("CLTask4").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("CLTask4").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("CLTask4").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val data = args(0)
    val df = spark.read.format("csv").option()

    spark.stop()
  }
}

