package com.bigdata.spark.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object map21 {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("map21").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("map21").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("map21").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val data = "file:///E:\\Kartik\\Work\\datasets\\sachinunst.txt"
    val srdd = sc.textFile(data)
    val sprocess = srdd.flatMap(x => x.split(" ")).map(x => (x,1)).reduceByKey((a,b)=>a+b).sortByKey(x => x._
      sprocess.take(10).foreach(println)
    spark.stop()
  }
}

