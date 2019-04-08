package com.bigdata.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object companiesjson {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("companiesjson").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("companiesjson").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("companiesjson").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
    val data = "file:///E:\\Kartik\\Work\\datasets\\companies.json"
    val cmpdf = spark.read.format("json").option("inferSchema","true").load(data)
    cmpdf.createOrReplaceTempView("companies")
    //val res = spark.sql("select _id.`$oid` ID,concat_ws('-',a.acquired_day,a.acquired_month,a.acquired_year) as Date,a.company.name cname, a.company.permalink link, a.price_amount price, a.price_currency_code ccode,a.source_description source,a.source_url url, a.term_code tcode from companies lateral view explode(acquisitions) t as a limit 50")
    val qres = spark.sql("select _id.`$oid` ID,a.acquired_day Day,a.acquired_month Month,a.acquired_year,a.company.name cname, a.company.permalink link, a.price_amount price, a.price_currency_code ccode,a.source_description source,a.source_url url, a.term_code tcode,concat_ws('-',fr.funded_day,fr.funded_month,fr.funded_year) as funddate,fr.id frid,fr.i.company.name Iname,fr.i.company.permalink plink ,fr.i.financial_org.name Fname,fr.i.financial_org.permalink flink,fr.i.person.first_name firstname,fr.i.person.last_name lastname,fr.i.person.permalink personlink,fr.raised_amount amount, fr.raised_currency_code currencycoderaised,fr.round_code rc,fr.source_description sd,fr.source_url surl,homepage_url from companies lateral view explode(acquisitions) t as a lateral view explode(funding_rounds) f as fr lateral view explode(investments) temp as i  limit 10")

    qres.show()
    qres.printSchema()
    val mhost = "jdbc:mysql://sqlinstance.cqccpxhuyrd3.ap-south-1.rds.amazonaws.com:3306/sqldb"
    val mprop = new java.util.Properties()
    mprop.setProperty("user", "musername")
    mprop.setProperty("password", "mpassword")
    mprop.setProperty("driver", "com.mysql.cj.jdbc.Driver")
    qres.write.mode(SaveMode.Overwrite).jdbc(mhost, "companynew", mprop)
    //val res = spark.sql("select id.`$oid` ID,acquisitions.acquired_day Day, acquisitions.acquired_month,acquisitions.acquired_year Year), acquisitions.acquiring_company Company, ac.name companyname,ac.permalink companylink,acquisitions.price_amount price,acquisitions.price_currency_code code,acquisitions.source_description source,acquisitions.source_url url,acquisitions.term_code termcode from companies lateral view explode(company) t as ac)")
    //res.show()
//    cmpdf.show()
    cmpdf.printSchema()
    spark.stop()
  }
}

