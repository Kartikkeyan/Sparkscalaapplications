package com.bigdata.spark.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object olympiccsv {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("olympiccsv").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("olympiccsv").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("olympiccsv").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val data = "file:///E:\\Kartik\\Work\\datasets\\athlete_events.csv"
    val olympicrdd = sc.textFile(data)
    val splitc = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
    val header = olympicrdd.first()
    //val olympicprocess = olympicrdd.filter(x => x!= header).map(x => x.split(splitc)).map(x =>  (x(0).replaceAll("\"","").trim.toInt,x(1).replaceAll("\"","").trim.replace(" ",""),x(2).replaceAll("\"",""),x(3).replaceAll("\"",""),x(4).replaceAll("\"",""),x(5).replaceAll("\"",""),x(6).replaceAll("\"",""),x(7).replaceAll("\"",""),x(8).replaceAll("\"",""),x(9).replaceAll("\"",""),x(10).replaceAll("\"",""),x(11).replaceAll("\"",""),x(12).replaceAll("\"","").replace("-",""),x(13).replaceAll("\"","").replace("-",""),x(14).replaceAll("\"","")))
    val olympicprocess = olympicrdd.filter(x => x!= header).map(x => x.split(splitc)).map(x =>  (x(0).replaceAll("\"","").trim.toInt,x(1).replaceAll("\"","").trim.replace(" ",""),x(2).replaceAll("\"",""),x(3).replaceAll("\"",""),x(4).replaceAll("\"",""),x(5).replaceAll("\"",""),x(6).replaceAll("\"",""),x(7).replaceAll("\"",""),x(8).replaceAll("\"",""),x(9).replaceAll("\"",""),x(10).replaceAll("\"",""),x(11).replaceAll("\"",""),x(12).replaceAll("\"","").replace("-",""),x(13).replaceAll("\"","").replace("-",""),x(14).replaceAll("\"",""))).filter(x => (x._15 == "Gold")).filter(x => x._4.min)

    olympicprocess.take(10).foreach(println)
    spark.stop()
  }
}

