name := "bts-rtda-lab-8" //Name of app.
version := "0.1.1" //Version aof app
scalaVersion := "2.11.0" // Scala version to be used

/*
  "sbt package" command will produce the java executable file:
    target/scala-2.11/<name><scalaVersion>-<version>.jar

  In this case:
  target/scala-2.11/bts-rtda-lab-5_2.11-0.1.1.jar
*/

val sparkVersion = "2.4.3" //Version of scala to be used on app

val sparkTestingBase = "2.4.3_0.12.0" //version to be used on package "spark-testing-base"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion, //Core library of spark
  "org.apache.spark" %% "spark-sql" % sparkVersion, //Core library of spark
  "org.apache.spark" %% "spark-hive" % sparkVersion % Test,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % Test,
  "org.apache.spark" %% "spark-tags" % sparkVersion % Test,
  "org.apache.kafka" % "kafka-clients" % "2.2.0",
  "org.elasticsearch" %% "elasticsearch-spark-20" % "7.1.1",
  "org.scalatest" %% "scalatest" % "3.0.5" % Test, //Basic test library of scala
  "com.holdenkarau" %% "spark-testing-base" % sparkTestingBase % Test //Custom test library for spark
)

//  SBT testing java options are too small to support running many of the tests due to the need to
//  launch Spark in local mode. Need to be increased
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")