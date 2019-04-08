package com.bigdata.sparksql

//import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object rddtodataframe {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("rddtodataframe").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("rddtodataframe").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("rddtodataframe").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    /*val data = "file:///E:\\Kartik\\Work\\datasets\\us-500.csv"
    val dfrdd = sc.textFile(data)
    val head = dfrdd.first()
    val splitc = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
   // val dfprocess = dfrdd.filter(x => x != head).map(x => x.split(splitc)).map(x => (x(0).replaceAll("\"",""),x(4).replaceAll("\"",""),x(5).replaceAll("\"",""),x(6).replaceAll("\"",""),1))
     val dfprocess = dfrdd.filter(x => x != head).map(x => x.split(splitc)).map(x => (x(6).replaceAll("\"",""),1)).reduceByKey((x,y) => x+y).sortByKey(x =>
*/
    /*val dfdata = "file:///E:\\Kartik\\Work\\datasets\\us-500.csv"
    val dfrdd = sc.textFile(dfdata)
    val splitc = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
    val head = dfrdd.first()
    val dfconv = dfrdd.filter(x => x != head).map(x => x.split(splitc)).map(x => (x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11))).toDF("first_name","last_name","company_name","address","city","county","state","zip","phone1","phone2","email","web")
    dfconv.createOrReplaceTempView("emp")

    import org.apache.spark.sql.functions

    //sql queries
    val res = spark.sql("select count(*) noofemployees,city from emp group by city order by noofemployees desc")
    //dsl queries
    val res = dfconv.groupBy($"city").agg(count("*")).alias*/
    import org.apache.spark.sql.types._

    val cols = "firstname,lastname,companyname"
    //val fields = cols.split(",").map(x => StructField(x, StringType, nullable = true))


