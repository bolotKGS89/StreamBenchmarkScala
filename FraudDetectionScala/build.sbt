ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.0"

resolvers += "Maven Central" at "https://repo.maven.apache.org/maven2/"
resolvers += "Google Maven Repository" at "https://maven.google.com"
resolvers += "jcenter" at "https://jcenter.bintray.com"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.2",
  "org.apache.spark" %% "spark-sql" % "3.3.2",
  "org.apache.spark" %% "spark-streaming" % "3.3.2",
  "org.apache.logging.log4j" %% "log4j-api-scala" % "12.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.20.0",
  "org.apache.storm" % "storm-core" % "2.4.0"
)

lazy val root = (project in file("."))
  .settings(
    name := "FraudDetectionScala"
  )
