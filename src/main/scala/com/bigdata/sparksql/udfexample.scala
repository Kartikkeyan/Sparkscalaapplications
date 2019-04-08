package com.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object udfexample {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("udfexample").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("udfexample").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("udfexample").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val udfdata = "file:///E:\\Kartik\\Work\\datasets\\us-500.csv"
    val userdf = spark.read.format("csv").option("header","true").option("inferSchema","true").load(udfdata)
    //val res = userdf.withColumn("phone1",when($"phone1".contains("-"),"")).
      val res1 = userdf.select($"first_name",$"last_name",lpad($"zip",len = 6,pad="0"))
     // val res1 = userdf.withColumn("first_name",lit(10))
    //select (firstname,phone1,case state when 'NY' then "New York' else state end Newstate from tablename)
    /*def usoffer(st : String)= st match (
    case "NY" => "10% off"
    case "OH" => "20% off"
    case "NJ" => "30% off"
    case  "_" => "no offer"
    )
*/
    val usersdf = List(("Kartik","Chennai","2533665"),("Albert","Delhi","565659")).toDF("name","city","phone").withColumn("zip",lit(641))
    //usersdf.withColumn("name",udfCha
      //res1.show(false)
    //userdf.show(false)
    spark.stop()
  }
}

