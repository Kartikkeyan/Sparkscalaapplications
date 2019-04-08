package com.bigdata.spark.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object store {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("store").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("store").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("store").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val storedat = "file:///E:\\Kartik\\Work\\datasets\\stores data-set.csv"
    val storerdd = sc.textFile(storedat)
    val splitc = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
    val storeprocess = storerdd.map(x => x.split(splitc)).map(x => (x,1)).map(x => (x(1),x(2)).toString())
    storeprocess.take(10).foreach(println)
    spark.stop()
  }
}

