package com.bigdata.spark.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object joins {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("wc").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("wc").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
/*
    val sqlContext = spark.sqlContext
*/
    import spark.implicits._
    import spark.sql
    val data1 = "file:///E:\\Kartik\\Work\\datasets\\asl.txt"
    val data2 = "file:///E:\\Kartik\\Work\\datasets\\asldata.txt"
    val drdd1 = sc.textFile(data1)
    val drdd2 = sc.textFile(data2)
    val head1 = drdd1.first()
    val head2 = drdd2.first()
    //cleaning the data removing header
   // val processdrdd1 = drdd1.filter(x => x!= head1).map(x => x.split(",")).map(x => (x(0),x(1).,x(2)))
    val processdrdd2 = drdd2.filter(x => x!= head2).map(x => x.split(",")).map(x => (x(0),x(1).toInt,x(2)))
    // filtering -    val processdrdd1 = drdd1.filter(x => x!= head1).map(x => x.split(",")).map(x => (x(0),x(1).toInt,x(2))).filter(x => (x._2 > 20 && x._3 == "chennai"))
    // before joining ---select the columns you want to join on

   // val key1 = processdrdd1.keyBy(x => x._1)
    val key2 = processdrdd2.keyBy(x => x._1)
   // val drddjoin = key1.join(key2)
    //drddjoin.collect.foreach(println)
    //processdrdd1.collect.foreach(println)
    processdrdd2.collect.foreach(println)

    spark.stop()
  }
}

