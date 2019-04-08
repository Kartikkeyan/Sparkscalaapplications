package com.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object bigdatatweets {
  def main(args: Array[String]) {
      val spark = SparkSession.builder.master("local[2]").appName("kafka_wordcount").getOrCreate()//local(2) 2 threads
      import spark.implicits._
      import spark.sql

      val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

      val APIkey = "yVxL15u14q4m9UCtCCK31J78V"//APIKey
      val APIsecretkey = "0TFECpbVVelj1A8ueeruPbiVohKodD8ADTdIHmdWWxYM0AtOR7"// (API secret key)
      val Accesstoken = "281390520-UNPHlghLEySZgn3CoCPh4Q0MGIsmXBvx356X6Rqj" //Access token
      val Accesstokensecret = "N8YBJ1CyeOHWf6oNaMeNWIQbLnu9PVuBnFugTWbJZqi20" //Access token secret

      val searchFilter = "bigdata,hadoop,spark" //keywords search
      val interval = 10

    //  import spark.sqlContext.implicits._
    System.setProperty("twitter4j.oauth.consumerKey", APIkey)
    System.setProperty("twitter4j.oauth.consumerSecret", APIsecretkey)
    System.setProperty("twitter4j.oauth.accessToken", Accesstoken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", Accesstokensecret)

    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(interval))
    val tweetStream = TwitterUtils.createStream(ssc, None, Seq(searchFilter.toString)) // create dstreams

    tweetStream.foreachRDD {x =>
      val data = x.toString()
      val sparkstr = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate() //gathering all configuration from existent
      import spark.implicits._
      val tweetsdf = x.map(a => (a.getId,a.getText,a.getFavoriteCount)).toDF("tweetID","tweettext","favcount")
      tweetsdf.show(false)
      val mhost = "jdbc:mysql://mydbapp.cqccpxhuyrd3.ap-south-1.rds.amazonaws.com:3306/sqldb"
      val mprop = new java.util.Properties()
      mprop.setProperty("user","musername")
      mprop.setProperty("password","mpassword")
      mprop.setProperty("driver","com.mysql.cj.jdbc.Driver")
      tweetsdf.write.mode(SaveMode.Append).jdbc(mhost,"bigdatatweets",mprop)
}
// now tweetStream is dstream
ssc.start()
ssc.awaitTermination()
spark.stop()
}
}

