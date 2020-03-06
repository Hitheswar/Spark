package com.dama.spark.dataFrame

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import src.main.scala.com.dama.spark.core.SparkApp

//ways to create dataframe
object CreateDataframe extends App with SparkApp{
  import sparkSession.implicits._
  lazy val sc = sparkSession.sparkContext

  val columns = Seq("language","user_account")
  val data = Seq(("java","2000"),("python","3000"),("scala","4000"),("sql","6000"))

  //Create Spark DataFrame from RDD
  val rdd = sc.parallelize(data)

  rdd.toDF().printSchema()
  /*root
  |-- _1: string (nullable = true)
  |-- _2: string (nullable = true)*/

  rdd.toDF("language","user_account").printSchema()
  /*root
  |-- language: string (nullable = true)
  |-- user_account: string (nullable = true)*/

  rdd.toDF(columns:_*).printSchema()
  /*
  root
 |-- language: string (nullable = true)
 |-- user_account: string (nullable = true)*/

  sparkSession.createDataFrame(rdd).toDF(columns:_*).printSchema()
  /*
  root
 |-- language: string (nullable = true)
 |-- user_account: string (nullable = true) */

  val schema = StructType(columns.map(fieldName => StructField(fieldName,StringType,true)))
  val rowRDD = rdd.map(attributes => Row(attributes._1,attributes._2))
  sparkSession.createDataFrame(rowRDD,schema).show()


}
