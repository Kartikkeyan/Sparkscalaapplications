package com.bigdata.sparkstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object kafkaconsumernew {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("kafkaconsumernew").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("kafkaconsumernew").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("kafkaconsumernew").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val topics = "indpak"
    val brokers = "localhost:9092"
    val topicsSet = topics.split(",").toSet
    /*   val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
             "bootstrap.servers"->"localhost:9092",
             "zookeeper.connect" -> "localhost:2181",
             "group.id" -> "kaf",
             "key.serializer"->"org.apache.kafka.common.serialization.StringSerializer",
             "value.serializer"->"org.apache.kafka.common.serialization.StringSerializer",
             "key.deserializer"->"org.apache.kafka.common.serialization.StringDeserializer",
             "value.deserializer"->"org.apache.kafka.common.serialization.StringDeserializer")*/
    val kafkaParams = Map[String, Object](
          "bootstrap.servers" -> "localhost:9092",
          "key.deserializer" -> classOf[StringDeserializer],
          "value.deserializer" -> classOf[StringDeserializer],
          "group.id" -> "use_a_separate_group_id_for_each_stream",
          "auto.offset.reset" -> "latest",
          "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val lines = KafkaUtils.createDirectStream[String, String](
          ssc,LocationStrategies.PreferConsistent,         // this is to run the
          ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    val msg = lines.map(x=>x.value())
        //msg.print()

        if (msg != null){msg.print()}
        msg.foreachRDD { x =>
          println(s"processing first line : $x")
          val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
          import spark.implicits._
          val df = spark.read.json(x)

          /*      val df = x.map(x => x.value).map(x => x.split(",")).map(x => (x(0), x(1), x(2), x(3))).toDF("id", "fname", "lname", "email")*/
          // val df = spark.read.format("csv").schema(dfschema).load(data)
          df.show()
          df.printSchema()
          /*df.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra")
            .option("table", "kafkacass").option("keyspace", "cassdb").save()*/

          //lines.print(
        }
        ssc.start()             // Start the computation
        ssc.awaitTermination()  // Wait for the computation to terminate

        //spark.stop()

    }}

    //spark.stop()



