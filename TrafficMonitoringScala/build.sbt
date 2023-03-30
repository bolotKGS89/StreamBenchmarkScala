ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

resolvers += "Maven Central" at "https://repo.maven.apache.org/maven2/"
resolvers += "Open Source Geospatial Foundation Repository" at "https://download.osgeo.org/webdav/geotools/"
resolvers += "osgeo" at "https://repo.osgeo.org/repository/release/"
resolvers += "GeoSolutions" at "https://maven.geo-solutions.it/"
resolvers += "JCenter" at "https://jcenter.bintray.com/"
resolvers += "Sonatype Nexus" at "https://oss.sonatype.org/content/repositories/releases/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.2",
  "org.apache.spark" %% "spark-sql" % "3.3.2",
  "org.apache.spark" %% "spark-streaming" % "3.3.2",
  "org.apache.logging.log4j" %% "log4j-api-scala" % "12.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.20.0",
  "org.geotools" % "gt-shapefile" % "25.0",
  "org.postgis" % "postgis-jdbc" % "1.3.3"
)

lazy val root = (project in file("."))
  .settings(
    name := "TrafficMonitoringScala"
  )
