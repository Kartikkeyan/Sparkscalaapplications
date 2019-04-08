package com.bigdata.sparkstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.kafka010.KafkaUtils._


object SparkKafkaConsumer {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("SparkKafkaConsumer").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[2]").appName("SparkKafkaConsumer").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("SparkKafkaConsumer").setMaster("local[*]")
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

      val lines = KafkaUtils.createDirectStream[String, String](   //dstream
      ssc,
      LocationStrategies.PreferConsistent,         // this is to run the
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
      //val lines = messages  // lines is a dStream  map(x=>x.value) or map(x=>x.key)
    // val data = args(0)

      lines.foreachRDD { x =>
      println(s"processing first line : $x")
      val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      //val df1=spark.read.format("csv").option("delimiter"," ").load(data)
      //val dfschema=df1.schema()
      val df = x.map(x=>x.value).map(x => x.split(",")).map(x => (x(0), x(1), x(2),x(3))).toDF("id", "fname", "lname","email")      // val df = spark.read.format("csv").schema(dfschema).load(data)
      df.show(false)
      /*val mdf=df.where($"city"==="mas")
      val hdf=df.where($"city"==="hyd")*/
       // df.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").option("table","kafkanifi").option("keyspace","cassandradb").save()

      /*val oUrl = "jdbc:oracle:thin://@oracledb.czzw4mblymse.ap-south-1.rds.amazonaws.com:1521/ORCL"
      val oProp = new java.util.Properties()
      oProp.setProperty("user","ousername")
      oProp.setProperty("password","opassword")
      oProp.setProperty("driver","oracle.jdbc.OracleDriver")
      //df.createOrReplaceTempView("personal")
      df.write.mode("append").jdbc(oUrl,"kafkalogssunil",oProp)*/
    }
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

    spark.stop()
  }
}

