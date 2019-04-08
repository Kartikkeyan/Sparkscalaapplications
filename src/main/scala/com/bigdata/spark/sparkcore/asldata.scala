package com.bigdata.spark.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object asldata {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("asldata").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("asldata").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("asldata").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val asldata = "file:///E:\\Kartik\\Work\\datasets\\asldata.txt"
    val aslrdd = sc.textFile(asldata)
    val head = aslrdd.first()
    val aslprocess = aslrdd.filter(x => x!= head).map(x => x.split(",")).map(x => (x(0),x(1),x(2).trim.toInt))
    aslprocess.collect.foreach(println)
    spark.stop()
  }
}

