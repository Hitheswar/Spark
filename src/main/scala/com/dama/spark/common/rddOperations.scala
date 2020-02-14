package com.dama.spark.common
import src.main.scala.com.dama.spark.core.SparkApp
import org.apache.log4j.{Level, Logger}

object rddOperations extends App with SparkApp{


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
  println(wordPairsRDD.collect.toList)
  val rbk = wordPairsRDD.reduceByKey(_ + _).collect()     //reduceByKey
  println(rbk.toList)

  val gbk = wordPairsRDD.groupByKey().map(t => (t._1, t._2.sum)).collect()    //groupByKey
  println(gbk.toList)

  println("***sample***")
  val x6 = sc.parallelize(Array(1, 2, 3, 4, 5))
  val y6 = x6.sample(false, 0.4)
  println(y6.collect.toList)

  println("***union***")
  val x7 = sc.parallelize(Array(1,2,3), 2)
  val y7 = sc.parallelize(Array(3,4), 1)
  val z7 = x7.union(y7)
  val z77 = z7.glom().collect()
  println(z77.toList)

  println("***join***")
  val x8 = sc.parallelize(Array(("a", 1), ("b", 2)))
  val y8 = sc.parallelize(Array(("a", 3), ("a", 4), ("b", 5)))
  val z8 = x8.join(y8)
  println(z8.collect().mkString(", "))

  println("***distinct***")
  val x9 = sc.parallelize(Array(1,2,3,3,4))
  val y9 = x.distinct()
  println(y9.collect().mkString(", "))

  println("***Aggregate***")
  def seqOp = (data:(Array[Int], Int), item:Int) => (data._1 :+ item, data._2 + item)
  def combOp = (d1:(Array[Int], Int), d2:(Array[Int], Int)) => (d1._1.union(d2._1), d1._2 + d2._2)
  val x10 = sc.parallelize(Array(1,2,3,4))
  val y10 = x10.aggregate((Array[Int](), 0))(seqOp, combOp)
  println(y10)

  println("***max***")
  val a1 = sc.parallelize(Array(2,4,1))
  val b1 = a1.max
  println(b1)

  println("***sum***")
  val a2 = sc.parallelize(Array(2,4,1))
  val b2 = a2.sum
  println(b2)

  println("***mean***")
  val a3 = sc.parallelize(Array(2,4,1))
  val b3 = a3.mean
  println(b3)

  println("***countByKey***")
  val a4 = sc.parallelize(Array(('J',"James"),('F',"Fred"), ('A',"Anna"),('J',"John")))
  val b4 = a4.countByKey()
  println(b4.toList)

  println("***saveAsTextFile***")
  val a5 = sc.parallelize(Array(2,4,1))
  a5.saveAsTextFile("H:\\STudy\\test\\text.txt")

}
