package com.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object importexport {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("importexport").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("importexport").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("importexport").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    //importing data from oracle
    val url = "jdbc:oracle:thin://@oracleinstance.cqccpxhuyrd3.ap-south-1.rds.amazonaws.com:1521/ORCL"
    val mprop = new java.util.Properties()
    mprop.setProperty("user","ousername")
    mprop.setProperty("password","opassword")
    mprop.setProperty("driver","oracle.jdbc.OracleDriver")
      //val dforacle = spark.read.jdbc(url, table = "EMPLYOEE", mprop)

    //import data based on query
      val query = "(select * from EMPP) tempemp"
      val odf = spark.read.jdbc(url, query, mprop)
      odf.createOrReplaceTempView("empl")
      //val res = spark.sql("select * from jobsnow")
      //res.show(truncate = false)
      val csvdata = "file:///E:\\Kartik\\Work\\datasets\\asl.txt"
      val csvdf = spark.read.format("csv").option("header","true").option("inferSchema","true").load(csvdata)
      csvdf.createOrReplaceTempView("csvtemp")
      val res = spark.sql("select * from empl e,csvtemp c where e.name = c.name")
    res.show(truncate = false)

    //export data
    //csvdf.write.jdbc(url,"asldat",mprop)
    res.write.jdbc(url,"ocsv",mprop)
      //dforacle.show()
    // default is parquet format
      //dforacle.write.format("csv").option("header","true").option("delimiter", "\t").save("E:\\Kartik\\Work\\datasets\\output\\oracle2")
    //dforacle.createOrReplaceTempView("emp")
      //val res = spark.sql("select ID,name from emp where salary>=200000")
      //res.show()
    spark.stop()
  }
}

