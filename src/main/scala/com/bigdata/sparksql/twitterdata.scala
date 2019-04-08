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
import java.util.concurrent.TimeUnit
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j._


object twitterdata {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("twitterdata").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[2]").appName("twitterdata").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("twitterdata").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

    val APIkey = "D280utnnYu8SjphIqFaQFhwpj"//APIKey
    val APIsecretkey = "mG2TymdLdZNGJiYZTfMr2m5W48tCrc3kmtCKn5CaGr3OcDdIvX"// (API secret key)
    val Accesstoken = "281390520-f96cFjcms5tbb7Wwt0FFgSd7karvvRLZKSdi2yBW" //Access token
    val Accesstokensecret = "lvx5djMdO1XLQlNvOBISPlJYqsoUCx2y7UdcpeznnN89M" //Access token secret

    val searchFilter = "big data,hadoop,apachespark,hive,Bigdata" //keywords search
    val interval = 10
    //  import spark.sqlContext.implicits._
    System.setProperty("twitter4j.oauth.consumerKey", APIkey)
    System.setProperty("twitter4j.oauth.consumerSecret", APIsecretkey)
    System.setProperty("twitter4j.oauth.accessToken", Accesstoken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", Accesstokensecret)

    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(interval))
    val bigstream = TwitterUtils.createStream(ssc, None, Seq(searchFilter.toString)) // create dstreams

      bigstream.foreachRDD { x =>
        val data = x.toString()
        val sparkstr = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate() //gathering all configuration from existent---line 17
        import spark.implicits._
        val tweetsdf = x.map(a => (a.getGeoLocation,a.getPlace,a.getText,a.getUser)).toDF( "geoloc", "place","tweettext","user")
        //tweetsdf.show(false)
        tweetsdf.createOrReplaceTempView("bigdata")
        val res = spark.sql("select * from bigdata where tweettext like '%apachespark%'")
        res.show(false)
        val mhost = "jdbc:mysql://mydbapp.cqccpxhuyrd3.ap-south-1.rds.amazonaws.com:3306/sqldb"
        val mprop = new java.util.Properties()

        mprop.setProperty("user", "musername")
        mprop.setProperty("password", "mpassword")
        mprop.setProperty("driver", "com.mysql.cj.jdbc.Driver")
        tweetsdf.write.mode(SaveMode.Append).jdbc(mhost, "bigdatatweets", mprop)
        res.write.mode(SaveMode.Append).jdbc(mhost,"sparktweets",mprop)
      }
        ssc.start()
        ssc.awaitTermination()

    spark.stop()
    }
}

