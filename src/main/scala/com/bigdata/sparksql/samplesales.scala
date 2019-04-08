package com.bigdata.sparksql

import org.apache.hadoop.hdfs.web.resources.OverwriteParam
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object samplesales {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("samplesales").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("samplesales").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("samplesales").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val ourl = "jdbc:oracle:thin://@myoracledb.cqccpxhuyrd3.ap-south-1.rds.amazonaws.com:1521/ORCL"
    val oprop = new java.util.Properties()
    oprop.setProperty("user","ousername")
    oprop.setProperty("password","opassword")
    oprop.setProperty("driver","oracle.jdbc.OracleDriver")
    val salesdata = "file:///E:\\Kartik\\Work\\datasets\\sales_data_sample.csv"
    val salesdf = spark.read.format("csv").option("header","true").option("inferSchema","true").load(salesdata)
    salesdf.createOrReplaceTempView("Sales")
    val res = salesdf.filter($"SALES">5000.00).groupBy($"CITY").agg(collect_list($"PRODUCTCODE").alias("Items")).withColumn("Items",concat_ws(",",$"Items")).sort($"Items")
    //res.select($"City",$"Items".substr(1,200).alias("Item")).write.jdbc(ourl,"samplesales",oprop)
    res.write.jdbc(ourl,"samplesales",oprop)
    res.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header","true").save("E:\\Kartik\\Work\\output")
    //val res = spark.sql("select collect_list(PRODUCTCODE) Noofproducts,CITY from Sales group by CITY having SALES > 5000.00 order by Noofprodcuts")
    res.show(false)
    salesdf.show(false)
    spark.stop()
  }
}
