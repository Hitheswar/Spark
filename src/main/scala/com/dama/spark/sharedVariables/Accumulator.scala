package com.dama.spark.sharedVariables
/*
Accumulators are the write only variables which are initialized once and sent to the workers.
These workers will update based on the logic written and sent back to the driver which will aggregate or process based on the logic.

Only driver can access the accumulator’s value. For tasks, Accumulators are write-only. For example,
it is used to count the number errors seen in RDD across workers.
 */
import src.main.scala.com.dama.spark.core.SparkApp

object Accumulator extends App with SparkApp{

  lazy val sc = sparkSession.sparkContext
  val myRDD = sc.parallelize(Array("This is a","is a gossip","this is introvert","this is introvert","is a"))


  val blank_line_accumulator = sc.accumulator(0,"Blank Lines")
  println("blank_line_accumulator.value  "+blank_line_accumulator.value)

  val input_file_blank_line_count = sc.textFile("H:\\Extras\\DaTA\\test\\line_count.txt").foreach{x => if(x.length() == 0) blank_line_accumulator += 1 }

  println("The total Blank Lines in the dataset linecount file is: " +blank_line_accumulator.value)


}
