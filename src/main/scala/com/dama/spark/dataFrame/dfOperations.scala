package com.dama.spark.dataFrame

import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import src.main.scala.com.dama.spark.core.SparkApp

object dfOperations extends App with SparkApp{

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  rootLogger.setLevel(Level.INFO)
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = sparkSession

  var matches_df = spark.read.format("com.databricks.spark.csv") .option("inferSchema", "false").option("header", true).option("delimiter", ",").load("H:\\Extras\\DaTA\\ipldata\\matches.csv")
  var deliveries_df = spark.read.format("com.databricks.spark.csv") .option("inferSchema", "false").option("header", true).option("delimiter", ",").load("H:\\Extras\\DaTA\\ipldata\\deliveries.csv")
  matches_df.show(5)
  deliveries_df.show(5)
  matches_df.createOrReplaceTempView("matches")
  deliveries_df.createOrReplaceTempView("deliveries")
  spark.sql("select * from matches limit 5").show()
}
