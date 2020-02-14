name := "Spark-Scala"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark"  %%  "spark-core"    % "2.4.4",
  "org.apache.spark"  %%  "spark-sql"     % "2.4.4"
)
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.4"

