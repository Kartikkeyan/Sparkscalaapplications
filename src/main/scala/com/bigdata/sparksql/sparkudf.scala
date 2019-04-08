package com.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object sparkudf {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("sparkudf").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("sparkudf").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("sparkudf").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val data = "file:///E:\\Kartik\\Work\\datasets\\us-500.csv"
    val udfdf = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
    udfdf.createOrReplaceTempView("usdata")
    val res = spark.sql("select * from usdata where state == 'NJ'")
    //udfdf.show(truncate = false)
    //res.show(truncate = false)
    //withcolumn used to create new column, lit used as a default value in the coulmn
    val up = udfdf.withColumn("Age",lit(18))
    //to replace '-' in the phone numbers use when clause
    //val rem = udfdf.withColumn("phone1",when ($"phone1".contains("-"),""))
    val rem = udfdf.withColumn("state",when ($"state"==="NY","New York").when($"state"==="OH","Ohio").otherwise($"state")
      //rem.show(truncate = false)
    //val res = udfdf.select(concat_ws(" ",$"last_name",$"last_name")).alias("fullname"),$"city",$"county")
    //val res1 = udfdf.select(lpad($"zip",len = 7,pad = "0"))
    //res1.show(truncate = false)
    //def usoffer(st: String) = st match(
      case "NY" => "10% off"
      case "OH" => "20% off"
      case "NJ" => "30% off"
      case  "_" => "no offer"
    )
    spark.stop()
  }
}

