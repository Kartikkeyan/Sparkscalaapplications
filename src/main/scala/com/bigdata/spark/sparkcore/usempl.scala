package com.bigdata.spark.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object usempl {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("usempl").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("usempl").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("usempl").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val usdata = "file:///E:\\Kartik\\Work\\datasets\\us-500.csv"
    val usrdd  = sc.textFile(usdata)
    val splitc = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
    val header = usrdd.first()
    val usprocess = usrdd.filter(x => x!=header).map(x => x.split(splitc)).map(x => (x(0).replaceAll("\"",""),x(1).replaceAll("\"",""),x(2).replaceAll("\"",""),x(3).replaceAll("\"",""),x(4).replaceAll("\"",""),x(5).replaceAll("\"",""),x(6).replaceAll("\"",""),x(7).replaceAll("\"",""),x(8).replaceAll("\"","").trim.replace("-",""),x(9).replaceAll("\"","").trim.replace("-",""),x(10).replaceAll("\"",""),x(11).replaceAll("\"",""))).filter(x => x._7 == "NY")
   // val usprocess = usrdd.filter(x => x!=header).map(x => x.split(splitc)).map(x => (x(0).replaceAll("\"",""),x(1).replaceAll("\"",""),x(2).replaceAll("\"",""),x(3).replaceAll("\"",""),x(4).replaceAll("\"",""),x(5).replaceAll("\"",""),x(6).replaceAll("\"",""),x(7).replaceAll("\"",""),x(8).replaceAll("\"","").trim.replace("-",""),x(9).replaceAll("\"","").trim.replace("-",""),x(10).replaceAll("\"",""),x(11).replaceAll("\"",""))).filter(x => x._11.contains("cox"))
   // val usdf = usrdd.filter(x => x!=header).map(x => x.split(splitc)).map(x => (x(0).replaceAll("\"",""),x(1).replaceAll("\"",""),x(2).replaceAll("\"",""),x(3).replaceAll("\"",""),x(4).replaceAll("\"",""),x(5).replaceAll("\"",""),x(6).replaceAll("\"",""),x(7).replaceAll("\"",""),x(8).replaceAll("\"","").trim.replace("-",""),x(9).replaceAll("\"","").trim.replace("-",""),x(10).replaceAll("\"",""),x(11).replaceAll("\"",""))).toDF("firstname","lastname","companyname","address","city","county","state","zip","phone1","phone2","email",web")
    //usdf.show()
    //val usprocess = usrdd.filter(x => x!=header).map(x => x.split(splitc)).map(x => (x(6).replaceAll("\"",""),1)).reduceByKey((a,b) => a+b).sortBy(x => x._2, false)
    usprocess.take(5).foreach(println)
    spark.stop()
  }
}

