package com.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object processjsondata {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("processjsondata").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("processjsondata").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("processjsondata").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val jsondata = "file:///E:\\Kartik\\Work\\datasets\\zips.json"
    val jsondf = spark.read.format("json").option("inferSchema","true").load(jsondata)
    //cleaning process
    jsondf.createOrReplaceTempView("jsontab")
    //val res = spark.sql("select _id id, city, loc[0] longitude, loc[1] lattitude, pop, state from jsontab")
    val jsonres = spark.sql("select cast(_id as int) id, city, loc[0] longitude, loc[1] lattitude, pop, state from jsontab where city == 'BARRE'")//cleaning
    //select cast(_id as int) id -----> convert string to
    jsonres.show(false)
    val mhost = "jdbc:mysql://sqlinstance.cqccpxhuyrd3.ap-south-1.rds.amazonaws.com:3306/sqldb"
    val mprop = new java.util.Properties()
    mprop.setProperty("user", "musername")
    mprop.setProperty("password", "mpassword")
    mprop.setProperty("driver", "com.mysql.cj.jdbc.Driver")
    jsonres.write.mode(SaveMode.Append).jdbc(mhost, "jsondata", mprop)
    //res.write.mode(SaveMode.Append).jdbc(mhost,"sparktweets",mprop)
    //jsondf.show(false)

    jsondf.printSchema()

    spark.stop()
  }
}

