package com.dama.spark.dataFrame

import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import src.main.scala.com.dama.spark.core.SparkApp

object dfOperations extends App with SparkApp{


  val spark = sparkSession

  /*var matches_df = spark.read.format("com.databricks.spark.csv") .option("inferSchema", "false").option("header", true).option("delimiter", ",").load("H:\\Extras\\DaTA\\ipldata\\matches.csv")
  var deliveries_df = spark.read.format("com.databricks.spark.csv") .option("inferSchema", "false").option("header", true).option("delimiter", ",").load("H:\\Extras\\DaTA\\ipldata\\deliveries.csv")
  matches_df.show(5)
  deliveries_df.show(5)
  matches_df.createOrReplaceTempView("matches")
  deliveries_df.createOrReplaceTempView("deliveries")
  spark.sql("select * from matches limit 5").show()*/

  /* touch retailData.csv
    echo InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country >> retailData.csv
    echo 536365,85123A,WHITE HANGING HEART T-LIGHT HOLDER,6,2010-12-01 08:26:00,2.55,17850.0,United Kingdom >> retailData.csv
    echo 536365,71053,WHITE METAL LANTERN,6,2010-12-01 08:26:00,3.39,17850.0,United Kingdom >> retailData.csv
    echo 536365,84406B,CREAM CUPID HEARTS COAT HANGER,8,2010-12-01 08:26:00,2.75,17850.0,United Kingdom >> retailData.csv       */

/*  var retailDf =  spark.read.format("csv").option("header", "true").option("inferSchema", "true")
                  .load("H:\\STudy\\test\\csv\\retailData.csv")
  retailDf.createOrReplaceTempView("retail_data")
  val staticSchema = retailDf.schema
  println(staticSchema)*/

  import org.apache.spark.sql.functions.{window, column, desc, col}
 /* retailDf.selectExpr( "CustomerId", "(UnitPrice * Quantity) as total_cost", "InvoiceDate") .groupBy(  col("CustomerId"),
    window(col("InvoiceDate"), "1 day")) .sum("total_cost") .show(5)
*/







  var df = sparkSession.read.csv("H:\\STudy\\test\\tw\\DE_Scala_MovingAverageAssignment-20191210T051850Z-001\\DE_MovingAverageAssignment_scala\\src\\test\\resources\\input\\stock_prices\\0.csv").createOrReplaceTempView("stockPrices")














}
