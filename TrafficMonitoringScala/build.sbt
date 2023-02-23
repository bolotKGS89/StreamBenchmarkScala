ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.2",
  "org.apache.spark" %% "spark-sql" % "3.3.2",
  "org.apache.spark" %% "spark-streaming" % "3.3.2",
  "org.apache.logging.log4j" %% "log4j-api-scala" % "12.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.20.0"
//  "org.opengis" %% "gt-xml" % "11.1"
)

//libraryDependencies  += "org.geotools" % "gt-shapefile" % "23.5"


lazy val root = (project in file("."))
  .settings(
    name := "TrafficMonitoringScala"
  )
