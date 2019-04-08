package com.bigdata.spark.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object students {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("students").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("students").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("students").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val studfile = "file:///E:\\Kartik\\Work\\datasets\\StudentsPerformance.csv"
    val studrdd  = sc.textFile(studfile)
    val splitc = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
    val header = studrdd.first()
    val studprocess = studrdd.filter(x => x!= header ).map(x => x.split(splitc)).map(x => (x(1),x(2)))
    studprocess.take(5).foreach(println)
    spark.stop()
  }
}