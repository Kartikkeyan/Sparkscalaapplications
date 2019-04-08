package com.bigdata.sparksql
//com.bigdata.sparksql.importexportcsvoracle
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object importexportcsvoracle {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("importexportcsvoracle").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("importexportcsvoracle").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("importexportcsvoracle").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val ourl = "jdbc:oracle:thin://@oracleinstance.cqccpxhuyrd3.ap-south-1.rds.amazonaws.com:1521/ORCL"
    val oprop = new java.util.Properties()
    oprop.setProperty("user","ousername")
    oprop.setProperty("password","opassword")
    oprop.setProperty("driver","oracle.jdbc.OracleDriver")
    val oracledf = spark.read.jdbc(ourl,"DEPT",oprop)
    oracledf.createOrReplaceTempView("depart")
    //oracledf.show(truncate = false)
    //val empdata = "file:///E:\\Kartik\\Work\\datasets\\asl.txt"
    val empdata = args
    val empcsvdf = spark.read.format("csv").option("header","true").option("inferSchema","true").load(empdata)
    empcsvdf.createOrReplaceTempView("empl")
    val res = spark.sql("select * from depart d full join empl e on d.loc = e.city")
    res.write.jdbc(ourl,"csvjoin",oprop)
    res.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header","true").save("E:\\Kartik\\Work\\datasets\\output\\csv")
     //res.show(truncate = false)

    //empcsvdf.show(truncate = false)
    spark.stop()
  }
}

