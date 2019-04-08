package com.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object olympics {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("olympics").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("olympics").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("olympics").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val olympicdata = "file:///E:\\Kartik\\Work\\datasets\\athlete_events.csv"
    val olympicdf = spark.read.format("csv").option("header","true").option("inferSchema","true").load(olympicdata)
    olympicdf.show(false)
    spark.stop()
  }
}

