name := "Kartik"

version := "0.1"

scalaVersion := "2.11.8"
val sparkVersion = "2.3.0"
val flinkVersion = "1.4.2"
val mysqlVersion = "6.0.6"
val calciteVersion = "1.16.0"

// https://mvnrepository.com/artifact/org.scala-lang/scala-library
libraryDependencies += "org.scala-lang" % "scala-library" % "2.11.12"
// https://mvnrepository.com/artifact/org.scala-lang/scala-reflect
libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.11.12"
// https://mvnrepository.com/artifact/org.scala-lang/scala-compiler
libraryDependencies += "org.scala-lang" % "scala-compiler" % "2.11.12"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.0" % "provided"

// https://mvnrepository.com/artifact/org.scala-sbt/io
libraryDependencies += "org.scala-sbt" %% "io" % "1.3.0-M6"


libraryDependencies ++= Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion,
  "org.apache.flink" % "flink-core" % flinkVersion,
  "org.apache.flink" %% "flink-clients" % flinkVersion,
  "org.apache.flink" % "flink-jdbc" % flinkVersion,
  "mysql" % "mysql-connector-java" % mysqlVersion,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion)
//"org.apache.flink" %% "flink-table" %  flinkVersi