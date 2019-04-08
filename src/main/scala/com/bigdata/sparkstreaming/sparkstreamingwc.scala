package com.bigdata.sparkstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming._

object sparkstreamingwc {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("sparkstreamingwc").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("sparkstreamingwc").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("sparkstreamingwc").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val ssc = new StreamingContext(spark.sparkContext,Seconds(10))
    //copy the Public DNS (IPv4) from EC2 instance created as the hostname of socketTextStream
    val lines = ssc.socketTextStream("ec2-13-233-230-23.ap-south-1.compute.amazonaws.com", 1533)///dstream
    //rdd approach
   // val process = lines.flatMap(x => x.split(" ")).map(x => (x,1)).reduceByKey((a,b) => a+b)
    val process = lines.flatMap(x => x.split(" ")).map(x => (x,1)).reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Minutes(4), Seconds(30))//within this 30 seconds 3 10 secs batch is available
   //window duration --- long period to continue
    //In cricket max window duration is 50 overs
    //over is sliding interval and ball by ball is batch(micro batch)
    //In trading ---last 9 hours is window interval and every 15 min is
    lines.foreachRDD{ words =>
      val spark = SparkSession.builder.config(words.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      //val df = words.map(x => x.split(",")).map(x => (x(0),x(1),x(2))).toDF("name","age","city")

      val userSchema = new StructType().add("name", "string").add("age", "integer").add("city","integer")
      val df = words.map(x => x.split(",")).map(x => (x(0),x(1).toInt,x(2))).toDF("name","age","city")
      val ndf = df.schema
      df.show()
    }

    //process.print()
    //lines.print()

    ssc.start()
    ssc.awaitTermination()
    //spark.stop()
  }
}

