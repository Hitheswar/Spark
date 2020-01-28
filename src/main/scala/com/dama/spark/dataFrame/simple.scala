package com.dama.spark.dataFrame

import org.apache.log4j.{Level, Logger}
import src.main.scala.com.dama.spark.core.SparkApp

object simple extends App with SparkApp {

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  rootLogger.setLevel(Level.INFO)
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

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


}
