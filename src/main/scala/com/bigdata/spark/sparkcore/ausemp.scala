package com.bigdata.spark.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object ausemp {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("ausemp").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("ausemp").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("ausemp").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val empdata = "file:///E:\\Kartik\\Work\\datasets\\au-500.csv"
    val emprdd = sc.textFile(empdata)
    val splitc = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
    val header = emprdd.first()
    //val empprocess = emprdd.filter(x => x!=header).map(x => x.split(splitc)).map(x => (x(0).replaceAll("\"",""),x(4).replaceAll("\"",""))).reduceByKey((a,b)=>a+b)
    val empprocess = emprdd.filter(x => x!=header).map(x => x.split(splitc)).map(x => (x(4).replaceAll("\"",""),1)).reduceByKey((a,b)=>a+b)
    def add(firstvalue:Int,secondvalue:Int)=(
        firstvalue + secondvalue
      )
        add(5,3)
    empprocess.take(10).foreach(println)
    spark.stop()
  }
}

