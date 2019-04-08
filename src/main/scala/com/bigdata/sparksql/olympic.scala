package com.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object olympic {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("olympic").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("olympic").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("olympic").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val data = "file:///E:\\Kartik\\Work\\datasets\\athlete_events.csv"
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
    df.createOrReplaceTempView("olympics")
    //val res = spark.sql("select count(*) NoofFemaleMedalWinners from olympics where (Sex == 'F' AND Medal != 'NA')")
    val res1 = spark.sql("select count(Medal) Top, City from olympics where Medal != 'NA' group by City order by Top desc ")
    //val res2 = spark.sql("select count(Season)  from olympics group by Season order by numberofolympics desc")
    val res2 = df.groupBy($"Season").count.alias("Noofolympics").show(false)

      // res3.show(false)
    //res2.show(false)
    df.show(false)
    spark.stop()
  }
}

