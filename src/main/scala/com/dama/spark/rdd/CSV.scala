package com.dama.spark.rdd

import src.main.scala.com.dama.spark.core.SparkApp

object CSV extends App with SparkApp{
  val spark = sparkSession
  val projectPath = System.getProperty("user.dir")
  val file_path = projectPath+"/data/csv/"

  val csv_rdd = spark.sparkContext.textFile(file_path+"matches.csv")
  //csv_rdd.foreach(println)
  println(csv_rdd.count())
  println(csv_rdd.partitions.size)



}
