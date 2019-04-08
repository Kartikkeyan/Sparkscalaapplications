package com.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object sparkhive {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("sparkhive").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("sparkhive").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("sparkhive").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
   // val data = "s3://samplepocdata/dataset/json/"
    val data = args(0)
    val df = spark.read.format("json").option("inferSchema","true").load(data)
    //df.write.mode(SaveMode.Overwrite).option("path",'/home/ha)saveAsTable.(args(1))
    spark.stop()
  }
}

