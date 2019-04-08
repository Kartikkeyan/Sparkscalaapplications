package com.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object processcsvdata {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("processcsvdata").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("processcsvdata").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("processcsvdata").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val csvdata = "file:///E:\\Kartik\\Work\\datasets\\au-500.csv"
    val csvdf = spark.sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").load(csvdata)//.option("delimiter","!")
    csvdf.createOrReplaceTempView("ausdata")
    //val res = spark.sql("select count(*) noofemployees,city from ausdata group by city order by noofemployees desc")
   val res = spark.sql("select count(*) noofemployees,company_name from ausdata group by company_name order by noofemployees desc")

     //csvdf.coalesce(1).write.format("csv").options(Map("header" -> "true", "delimiter" -> "|")).save("E:\\Kartik\\Work\\datasets\\output1")
    res.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").options(Map("header" -> "true", "delimiter" -> " ")).save("E:\\Kartik\\Work\\datasets\\output2")
    res.show(5,false)
    //csvdf.printSchema()
    //csvdf.show(false)
    //println("Number of records")+ res.count().toString
    spark.stop()
  }
}

