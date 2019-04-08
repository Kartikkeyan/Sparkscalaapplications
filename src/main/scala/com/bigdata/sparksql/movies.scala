package com.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object movies {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("movies").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("movies").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("movies").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val moviedata = "file:///E:\\Kartik\\Work\\datasets\\movies.csv"
    val moviedf = spark.read.format("csv").option("header","true").option("inferSchema","true").load(moviedata)
    moviedf.createOrReplaceTempView("movies")
    //val res1 = spark.sql("select genre, count(*) numberofmovies from movies group by genre order by numberofmovies desc" )
    val res3 = spark.sql("select genre, (count(genre)/(select count(*) from movies)) percentage from movies group by genre order by percentage desc")
    //val res2 = spark.sql("select sum(numberofmovies), genre from movies")
    res3.show
   // res1.show(false)
   // val udf = moviedf.withColumn("Remarks",$"star_rating")
    /*def rating(x: Double):String = x match {
      case a if (a > 9.0) => "excellent"
      case b if (b > 8.5 && b <9) => "very good movie"
      case c if (c > 8 && c <8.5) => "good movie"
      case d if (d > 7.5 && d < 8.0) => "ok not bad"
      case e if (e > 7.0 && e < 7.5)=> "flop movie"
      case f if (f < 7.0) => "better not to release the movie"
      case _ => "No review"
    }


    val udfrating = udf(rating _)
    val res1 = moviedf.withColumn("Remarks",udfrating($"star_rating"))
    res1.show(false)*/

   /* def rating(x: Double):String = x match {
      case 9.0 | 10.0 => "excellent"
      /*case 8.5 to 9.0 => "very good movie"
      case 8.0 to 8.5 => "good movie"
      case 7.5 to 8.0 => "ok not bad"
      case 7.0 to 7.5 => "flop movie"
      case  6.5 to 7.0 => "better not to release the movie"*/
      case _ => "No review"
    }
    val udfrating = udf(rating _)
    val res = moviedf.withColumn("Remarks",udfrating($"star_rating"))
*/  /*val res = moviedf.groupBy($"duration").count.sort($"count".desc)
    res.show(false)*/
    moviedf.show(false)
    //val res = moviedf.groupBy($"duration").count.alias("Number").sort($"Number".desc).withColumn("Percentage",($"Number"))
    //val genredf = moviedf.groupBy("genre").count().alias("noofmovies").sort($"noofmovies".desc)//.as("noofmovies")//.withColumn("Percentage",($"noofmovies").)
   // val res = genredf.withColumn("Percentage",$"number")

   // genredf.show()
   // res.show(false)
    spark.stop()
  }
}

