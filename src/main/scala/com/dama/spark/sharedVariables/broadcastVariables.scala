package com.dama.spark.sharedVariables

import src.main.scala.com.dama.spark.core.SparkApp

import scala.reflect.io.File

object broadcastVariables extends App with SparkApp{

  lazy val sc = sparkSession.sparkContext
  val lookup = Map("This" -> "frequent", "is" -> "frequent", "my" -> "moderate", "file" -> "rare")

  val broadcastLookup = sc.broadcast(lookup)

  def lookupWord(word: String):(String, String) = (word, broadcastLookup.value.get(word).getOrElse("NA"))

  val myRDD = sc.parallelize(Array("This is a","is a gossip","this is introvert","this is introvert","is is a"))

  var result = myRDD.flatMap(line => if(line != "")line.split(" ") else Array[String]()).map(lookupWord).countByValue()
  result.foreach(println)
}
