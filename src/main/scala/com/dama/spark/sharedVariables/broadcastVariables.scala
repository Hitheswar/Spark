package com.dama.spark.sharedVariables
/*
Broadcast Variables are the read-only shared variables.
Suppose, there is a set of data which may have to be used multiple times in the workers at different phases,
we can share all those variables to the workers from the driver and every machine can read them.
 */
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
