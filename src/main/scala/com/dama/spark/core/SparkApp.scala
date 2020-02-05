package src.main.scala.com.dama.spark.core

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkApp extends App{

  lazy val sparkConf = new SparkConf().setAppName("Learn Spark").setMaster("local[*]").set("spark.cores.max", "2")

  lazy val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  rootLogger.setLevel(Level.INFO)
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

}
