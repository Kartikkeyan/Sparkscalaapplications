package com.bigdata.sparkstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object kafkaconsumerapicsv {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("kafkaconsumerapicsv").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("kafkaconsumerapicsv").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("kafkaconsumerapicsv").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    import scala.util.Try
    val topic = "indpak"
    val brokers = "localhost:9092"
    val topicsSet = topic.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
      "bootstrap.servers"->"localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kaf",
      "key.serializer"->"org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer"->"org.apache.kafka.common.serialization.StringSerializer",
      "key.deserializer"->"org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer"->"org.apache.kafka.common.serialization.StringDeserializer")
     val lines = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
       ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)) //dstream

      lines.foreachRDD { x =>
        println(s"processing first line : $x")
        val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
        import spark.implicits._
        val df = x.map(x => x.value).map(x => x.split(",")).map(x => (x(0), x(1), x(2), x(3), x(4), x(5))).toDF("id", "fname", "lname", "email", "gender", "ipaddress")
        df.show(false)
        val mhost = "jdbc:mysql://sqlinstance.cqccpxhuyrd3.ap-south-1.rds.amazonaws.com:3306/sqldb"
        val mprop = new java.util.Properties()
        mprop.setProperty("user", "musername")
        mprop.setProperty("password", "mpassword")
        mprop.setProperty("driver", "com.mysql.cj.jdbc.Driver")
        df.write.mode(SaveMode.Append).jdbc(mhost, "kafkaconsumercsv", mprop)

      }
    ssc.start()             // Start the computation
    ssc.awaitTermination()
    //spark.stop()
  }
}

