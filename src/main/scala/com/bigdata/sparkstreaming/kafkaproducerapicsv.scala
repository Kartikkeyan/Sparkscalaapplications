package com.bigdata.sparkstreaming

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object kafkaproducerapicsv {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("kafkaproducerapicsv").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[2]").appName("kafkaproducerapicsv").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("kafkaproducerapicsv").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val ssc = new StreamingContext(spark.sparkContext,Seconds (10))
    import spark.implicits._
    import spark.sql
    val path = "file:///E:\\Kartik\\Work\\nifidata"
    val topic = "indpak"
    val rdddata = sc.textFile(path)
    val head = rdddata.first()
    val data = rdddata.filter(x => x!= head)
    data.foreachPartition(rdd => {
      import java.util._

      val props = new java.util.Properties()
      //  props.put("metadata.broker.list", "localhost:9092")
      //      props.put("serializer.class", "kafka.serializer.StringEncoder")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("bootstrap.servers", "localhost:9092") //by default kafka port number 9092

      val producer = new KafkaProducer[String, String](props) //producer api
      rdd.foreach(x => {
        println(x)
        producer.send(new ProducerRecord[String, String](topic.toString(), x.toString)) //sending to kafka broker key,value
        //(topic, "venu,32,hyd")
        //(indpak,"anu,56,mas")
        Thread.sleep(5000)
      })
    })
      ssc.start() // Start the computation
      ssc.awaitTermination()
     //spark.stop()
  }
}

