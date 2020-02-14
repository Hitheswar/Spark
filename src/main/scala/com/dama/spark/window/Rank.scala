package com.dama.spark.window

import src.main.scala.com.dama.spark.core.SparkApp
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
object Rank extends App with SparkApp{
  import sparkSession.implicits._
  val simpleData = Seq(("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100))

  val df = simpleData.toDF("employee_name", "department", "salary")
  df.show()

  val windowSpec  = Window.partitionBy("department").orderBy("salary")
  df.withColumn("row_number",row_number.over(windowSpec))
    .show()
  /*  row_number
  +-------------+----------+------+----------+
|employee_name|department|salary|row_number|
+-------------+----------+------+----------+
|        James|     Sales|  3000|         1|
|        James|     Sales|  3000|         2|
|       Robert|     Sales|  4100|         3|
|         Saif|     Sales|  4100|         4|
|      Michael|     Sales|  4600|         5|
|        Maria|   Finance|  3000|         1|
|        Scott|   Finance|  3300|         2|
|          Jen|   Finance|  3900|         3|
|        Kumar| Marketing|  2000|         1|
|         Jeff| Marketing|  3000|         2|
+-------------+----------+------+----------+   */



  df.withColumn("rank",rank().over(windowSpec)).show()
}
