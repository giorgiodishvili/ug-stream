ThisBuild / version := "1.0-SNAPSHOT"

scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "stream",
    libraryDependencies ++= Seq(
      // Spark dependencies
      "org.apache.spark" %% "spark-core" % "2.4.3",
      "org.apache.spark" %% "spark-sql" % "2.4.3",
      "org.apache.spark" %% "spark-avro" % "2.4.3",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.3",
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.3",

      // MySQL Connector
      "mysql" % "mysql-connector-java" % "8.0.20"
    ),
    resolvers ++= Seq(
      "Maven Central" at "https://repo1.maven.org/maven2/"
    )
  )
