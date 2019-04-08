package com.bigdata.spark.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object mapusecase {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("mapusecase").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("mapusecase").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("mapusecase").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
/*    val data = "file:///E:\\Kartik\\Work\\datasets\\us-500.csv"
    val mrdd = sc.textFile(data)
    val mprocess = mrdd.map(x => x.split(','))
    mprocess.collect.foreach(println)*/
    spark.stop()
  }
}

