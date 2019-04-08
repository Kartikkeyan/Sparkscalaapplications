package com.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object datasetapi {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("datasetapi").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("datasetapi").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("datasetapi").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val winedata = "file:///E:\\Kartik\\Work\\datasets\\winemag-data-130k-v2.csv"
    val dfwd  = spark.sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").load(winedata)
    dfwd.createOrReplaceTempView("winetab")
    //val res = spark.sql("select count(*) noofusers ,country from winetab group by country order by noofusers desc ")
    //val res = spark.sql("select country, description,price,province from winetab where price >= 15.0")

    //res.show()
   dfwd.printSchema()
    dfwd.show()
    spark.stop()
  }
}

