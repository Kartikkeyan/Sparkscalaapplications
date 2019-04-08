package com.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object processcomplexjson {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("processcomplexjson").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("processcomplexjson").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("processcomplexjson").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val data = "file:///E:\\Kartik\\Work\\datasets\\world_bank.json"
    val cjsondf = spark.read.format("json").option("inferSchema","true").load(data)
    cjsondf.createOrReplaceTempView("cjsontab")
    //to process struct data column use parentcolumn.`childcolumn`
    //backquote `` is used to skip special characters
    //if only array use the position number in the array starting at 0
    //if struct within array use lateral view explode of complex column
    //val res = spark.sql("select _id.`$oid` oid,project_abstract.cdata cdata,mn.code majorsectorcode, mn.name majorsectorname from cjsontab lateral view explode(mjsector_namecode) t as mn")
    val cjsonres = spark.sql("select _id.`$oid` oid,project_abstract.cdata cdata,mn.code majorsectorcode, mn.name majsectorname,mj.name majorsectorname, mj.percent majorsectorpercent,mjtheme[0] theme,th.code themecode, th.name themename from cjsontab lateral view explode(mjsector_namecode) t as mn lateral view explode (majorsector_percent) m as mj lateral view explode (mjtheme_namecode) t as th limit 10 ")
    val mhost = "jdbc:mysql://sqlinstance.cqccpxhuyrd3.ap-south-1.rds.amazonaws.com:3306/sqldb"
    val mprop = new java.util.Properties()
    mprop.setProperty("user", "musername")
    mprop.setProperty("password", "mpassword")
    mprop.setProperty("driver", "com.mysql.cj.jdbc.Driver")
    cjsonres.write.mode(SaveMode.Append).jdbc(mhost, "cjsondata", mprop)
    cjsonres.show()
   // cjsonres.printSchema()
    //cjsondf.show(false)
    cjsondf.printSchema()
    spark.stop()
  }
}

