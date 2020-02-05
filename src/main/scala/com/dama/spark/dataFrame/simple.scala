package com.dama.spark.dataFrame

import org.apache.log4j.{Level, Logger}
import src.main.scala.com.dama.spark.core.SparkApp

object simple extends App with SparkApp {

  val spark = sparkSession

  val myRange = spark.range(100).toDF("id")
  myRange.show(5)

  val divsBy2 = myRange.where("id % 2 = 0")
  var count = divsBy2.count()
  println("divsBy2 count "+count)

  var summary_df = spark.read.format("com.databricks.spark.csv").option("inferSchema",true).option("header",true).load("H:\\STudy\\test\\csv\\summary.csv")
  summary_df.show()

  summary_df.take(2)

  summary_df.sort("count").explain()

  spark.conf.set("spark.sql.shuffle.partitions", "5")

  summary_df.sort("count").take(2)

  summary_df.createOrReplaceTempView("flightData")

  var dfWay = summary_df.groupBy("DEST_COUNTRY_NAME").count()
  dfWay.show()
  var sqlWay = spark.sql("select DEST_COUNTRY_NAME, count(1) from flightData group by DEST_COUNTRY_NAME")
  sqlWay.show()

  dfWay.explain()
  sqlWay.explain()

  spark.sql("select max(count) from flightData").show()

  import org.apache.spark.sql.functions.max
  summary_df.select(max("count")).show()

  var maxSql = spark.sql("select  DEST_COUNTRY_NAME, sum(count) as destinationTotal from flightData group By DEST_COUNTRY_NAME order " +
    "By sum(count) Desc limit 5")               //sqlWay
  maxSql.collect().foreach(println)

  import org.apache.spark.sql.functions.desc
  summary_df.groupBy("DEST_COUNTRY_NAME").sum("count").withColumnRenamed("sum(count)","destination_total")
    .sort(desc("destination_total")) .limit(5) .collect().foreach(println)      //df way
}
