package com.dama.spark.common
import src.main.scala.com.dama.spark.core.SparkApp
import org.apache.log4j.{Level, Logger}

object rddOperations extends App with SparkApp{

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  rootLogger.setLevel(Level.INFO)
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val sc = sparkSession.sparkContext

  println("***map***")
  val x = sc.parallelize(Array("rdd","df","ds","graphs","streaming","ML"))
  val y =x.map(x =>("spark",x))
  println(y.collect().toList)
  val y0 = x.map(("spark",_))                     //shorter syntax
  println(y0.collect.toList)

  println("***flatMap***")
  val x1 = sc.parallelize(Array("AsokNagar Chennai TamilNadu","KPR TPTY AndhraPradesh"))
  println(x1.collect().toList)
  val y1 = x1.map(x =>x).collect()
  println(y1.toList)              //List(AsokNagar Chennai TamilNadu, KPR TPTY AndhraPradesh)
  val y11 = x1.flatMap(x => x.split(" ")).collect()
  println(y11.toList)             //List(AsokNagar, Chennai, TamilNadu, KPR, TPTY, AndhraPradesh)
  val y12 = x1.flatMap(_.split(" ")).collect()
  println(y12.toList)             //List(AsokNagar, Chennai, TamilNadu, KPR, TPTY, AndhraPradesh)

  println("***filter***")
  val x2 = sc.parallelize(1 to 10,2)
  val y2 = x2.filter(x =>x%2==0).collect()
  println(y2.toList)                                  //List(2, 4, 6, 8, 10)
  val y22 = x2.filter(_%2 ==0).collect
  println(y22.toList)                                 //List(2, 4, 6, 8, 10)

  println("***Reduce***")
  val x3 = sc.parallelize(1 to 10, 2)
  val y3 = x3.reduce((accum,n) => (accum + n))
  println(y3)   //55
  val y33 = x3.reduce(_ + _)
  println(y33)    //55
  val y31  = x3.reduce(_ * _)
  println(y31)  //3628800

  println("***groupBy***")
  val x4 = sc.parallelize(Array("spark","scala","java","j2ee","python"))
  val y4 = x4.groupBy(w => w.charAt(0))
  println(y4.collect().toList)

  println("***groupByKey***")
  val x5 = sc.parallelize( Array(('B',5),('B',4),('A',3),('A',2),('A',1)))
  val y5 = x5.groupByKey(2).collect()
  println(y5.toList)

  println("***reduceByKey***")
  val words = Array("one", "two", "two", "three", "three", "three")
  val wordPairsRDD = sc.parallelize(words).map(word => (word, 1))


}
