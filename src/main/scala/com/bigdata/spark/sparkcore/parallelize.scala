package com.bigdata.spark.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object parallelize {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("parallelize").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("parallelize").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("parallelize").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    //val names = Array("kartik","venu","Sachin","john")
    val names = Array("kartik","venu","Sachin","john")
    //val names1 = Array("kartik kumar","venu katragadda","Sachin tendulkar","john abraham")
    val namesrdd = sc.parallelize(names)
      //names1.flatMap(x => x.split(" "))
      //names1.map(x => x.split(" ")).flatten
      val proces= namesrdd.map(x => x.concat(" text"))
    //names.map(x => x.contains("e"))
      proces.collect.foreach(println)
    spark.stop()

    val arr =Array("Mon","Tues","Wed","Thursday","Mon","Tues")
    val arrrdd = sc.parallelize(arr)
    val arrprocess = arrrdd.flatMap(x => x.split(" ")).map(x => (x,1)).reduceByKey((a,b) => (a+b)).sortBy(x => x._2)
    arrprocess.collect.foreach(println)
}

