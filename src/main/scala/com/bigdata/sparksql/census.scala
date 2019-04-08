package com.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object census {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("census").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("census").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("census").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val censusdata = "file:///E:\\Kartik\\Work\\datasets\\census_data.csv"
    val censusdf = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(censusdata)
    val res = censusdf.withColumn("occupation",when($"occupation".contains("-"),"").otherwise($"occupation"))
    //censusdf.show()
    res.show
    spark.stop()
  }
}

