package com.dama.spark.rdd

import src.main.scala.com.dama.spark.core.SparkApp

object TextFiles extends App with SparkApp{

  var projectPath = System.getProperty("user.dir")
  val file_path   = projectPath +"/data/set1/"
  val file_path2  = projectPath +"/data/set2"
  val rdd1 = sparkSession.sparkContext.textFile(file_path+"ipl_matches1.txt")
  rdd1.foreach(f =>{
   // println(f)
  })
  println(rdd1.count())   //reading one textfile

  lazy val sc = sparkSession.sparkContext
  val rdd2 = sc.textFile(file_path+"*") //Read all text files from a directory into a single RDD
  rdd2.foreach(f =>{
    //println(f)
  })
  println(rdd2.count())

  val rddWhole = sc.wholeTextFiles(file_path+"*") //returns an RDD[Tuple2]. where first value (_1) in a tuple is a file name and
                                                  // second value (_2) is content of the file.
  rddWhole.foreach(files =>{
    //println(files._1)
  })

  val multiText = sc.textFile(file_path+"ipl_matches*.txt") //Read all text files matching a pattern to single RDD
  //multiText.foreach(f =>println(f))
  //sc.wholeTextFiles(file_path+"ipl_matches*.txt").foreach(files =>println(files._1))


}
