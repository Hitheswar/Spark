package src.main.scala.com.dama.spark.util

import src.main.scala.com.dama.spark.core.SparkApp

object helpar extends App with SparkApp{

  val sc = sparkSession.sparkContext

  val rdd = sc.parallelize(Seq("spark","scala","java"))
  rdd.foreach(println)

  var summary_df = sparkSession.read.format("com.databricks.spark.csv").option("inferSchema",true).option("header",true).load("H:\\STudy\\test\\csv\\summary.csv")
  summary_df.show()
}
