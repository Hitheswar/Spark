package com.dama.spark.datasets
import src.main.scala.com.dama.spark.core.SparkApp
/*
The Datasets API provides the benefits of RDDs (strong typing, ability to use powerful lambda functions) with the benefits of
Spark SQL’s optimized execution engine. You can define a Dataset JVM objects and then manipulate them using functional transformations
(map, flatMap, filter, and so on) similar to an RDD. The benefits is that, unlike RDDs, these transformations are now applied on a structured and
strongly typed distributed collection that allows Spark to leverage Spark SQL’s execution engine for optimization.
 */
object CreateDataset extends App with SparkApp{

  import sparkSession.implicits._
  var ds = Seq(1,2,3).toDS()              // convert a sequence to a Dataset
  ds.show()

  case class Person(id: Int, name: String, age: Int)//If you have a sequence of case classes,
  // calling .toDS() provides a Dataset with all the necessary fields.
  case class Person2(city: String)
  val PersonDS = Seq(Person(1,"dama",23),Person(2,"Manoj",21),Person(3,"Buvan",18)).toDS()
  PersonDS.show()

  //Create a Dataset from an RDD

  val rdd = sparkSession.sparkContext.parallelize(Seq(("DHL"),("KDP")))

  val das = rdd.toDS().show()

  val ds2 = Seq(Person2("DHL"),Person2("KDP")).toDS()



  PersonDS.withColumnRenamed("name","FullName")
  PersonDS.withColumn("city",ds2.col("city")).show()








}
