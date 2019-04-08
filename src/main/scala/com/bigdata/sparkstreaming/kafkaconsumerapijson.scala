package com.bigdata.sparkstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.cql.CassandraConnector

import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object kafkaconsumerapijson {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("kafkaconsumerapijson").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("kafkaconsumerapijson").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("kafkaconsumerapijson").setMaster("local[*]")
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

      val message = lines.map(x=>x.value())
      if (message != null){message.print()}

      message.foreachRDD { x =>
        println(s"processing first line : $x")
        val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
        import spark.implicits._
        val df = spark.read.json(x)
        df.printSchema()
        df.createOrReplaceTempView("consumerjson")
        val res = spark.sql("select nationality Country,re.user.cell Mobile,cast(re.user.dob as string) Date,re.user.email Email,re.user.gender Gender,re.user.location.city city, re.user.location.state state, re.user.location.street street, cast(re.user.location.zip as string) zipcode,concat_ws(' ',re.user.name.first,re.user.name.last) Name, re.user.name.title title, re.user.phone phone, re.user.username username from consumerjson lateral view explode(results) t as re ")
        //val res = spark.sql("select nationality country,version from consumerjson")
       // res.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").option("table","kafkaconsumer").option("keyspace","cassandradb").save()
        res.show(false)
        res.printSchema()
        val mhost = "jdbc:mysql://sqlinstance.cqccpxhuyrd3.ap-south-1.rds.amazonaws.com:3306/sqldb"
        val mprop = new java.util.Properties()
        mprop.setProperty("user", "musername")
        mprop.setProperty("password", "mpassword")
        mprop.setProperty("driver", "com.mysql.cj.jdbc.Driver")
        res.write.mode(SaveMode.Append).jdbc(mhost, "kafkaconsumerjson", mprop)


       }
      ssc.start()             // Start the computation
      ssc.awaitTermination()//spark.stop()
  }
}

