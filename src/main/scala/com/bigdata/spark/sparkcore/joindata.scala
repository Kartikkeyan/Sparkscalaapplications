package com.bigdata.spark.sparkcore

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object joindata {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("joindata").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("joindata").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("joindata").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val jdata1 = "file:///E:\\Kartik\\Work\\datasets\\asl.txt"
    val jdata2 = "file:///E:\\Kartik\\Work\\datasets\\asldata.txt"

    val jrdd1 = sc.textFile(jdata1)
    val jrdd2 = sc.textFile(jdata2)

    val jhead1 = jrdd1.first()
    val jhead2 = jrdd2.first()
    //cleaning data --removing header
    //val jprocess1 = jrdd1.filter(x => x != jhead1)
    //val jprocess2 = jrdd2.filter(x => x != jhead2)

    //splitting the record
    /*val jprocess1 = jrdd1.filter(x => x != jhead1).map(x => x.split(","))
    val jprocess2 = jrdd2.filter(x => x != jhead2).map(x => x.split(","))
*/
    //converting to tuple---selecting all records
    /*val jprocess1 = jrdd1.filter(x => x != jhead1).map(x => x.split(",")).map(x => (x(0),x(1),x(2)))
    val jprocess2 = jrdd2.filter(x => x != jhead2).map(x => x.split(",")).map(x => (x(0),x(1),x(2)))
*/
    //filtering records based on conditions
    val jprocess1 = jrdd1.filter(x => x != jhead1).map(x => x.split(",")).map(x => (x(0),x(1).toInt,x(2)))//.filter(x => (x._2>30 && x._3 == "Chennai")
    //val jfilter =
   /* val jkey1 = jprocess1.keyBy(x => x._1)
    val jkey2 = jprocess2.keyBy(x => x._1)
    // inner joining two datasets
    val joinrdd = jkey1.join(jkey2).map(x => (x._1,x._2._1._3,x._2._2._2))
    // left outer join
    val joinrdd = jkey1.join(jkey2).map(x => (x._1,x._2._1._3,x._2._2._2))
*/
    //joinrdd.collect.foreach(println)
    jprocess1.collect.foreach(println)
    //jprocess2.collect.foreach(println)


    spark.stop()
  }
}

