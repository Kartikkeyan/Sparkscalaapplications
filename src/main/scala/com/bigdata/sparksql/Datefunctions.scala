package com.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object Datefunctions {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("Datefunctions").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("Datefunctions").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("Datefunctions").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    //val datedf = sc.parallelize(Seq(1)).toDF("id")
    val dtdata = "file:///E:\\Kartik\\Work\\datasets\\us-500.csv"
    val datedf = spark.read.format("csv").option("header","true").option("inferSchema","true").load(dtdata)
    //val res = datedf.withColumn("Newyorkstudents",when($"state"==="NY","Newyork student").otherwise($"state")).drop($"state").withColumnRenamed($"Newyorkstudents","state")
    val res1 = datedf.select($"first_name",$"city",regexp_replace($"phone1","-","").alias("phonenumber"))
    //res1.show()
    //datedf.show(false)
    /*val datajson = "file:///E:\\Kartik\\Work\\datasets\\world_bank.json"
    val jsondf = spark.read.format("json").option("inferSchema","true").load(datajson)
    jsondf.show(false)
    jsondf.printSchema()*/

    val recordsdata = "file:///E:\\Kartik\\Work\\datasets\\10000Records.csv"
    val recordsdf = spark.read.format("csv").option("header","true").option("inferSchema","true").load(recordsdata)
    //recordsdf.show(false)
    //val cols = recordsdf.columns.map(x => x.replaceAll("[^a-zA-Z]","")),""))


    val res2 = recordsdf.select($"Emp ID",$"First Name",regexp_extract($"state","NY",1))
    res2.show()
    spark.stop()
  }
}
