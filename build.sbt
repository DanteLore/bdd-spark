name := "spark-bdd-example"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "log4j" % "log4j" % "1.2.14",
  "org.apache.spark" %% "spark-core" % "1.6.0",
  "org.apache.spark" %% "spark-sql" % "1.6.0",
  "org.apache.spark" %% "spark-mllib" % "1.6.0",
  "org.json4s" %% "json4s-jackson" % "3.2.7",
  "info.cukes" % "cucumber-core" % "1.2.4" % "test",
  "info.cukes" %% "cucumber-scala" % "1.2.4" % "test",
  "info.cukes" % "cucumber-jvm" % "1.2.4" % "test",
  "info.cukes" % "cucumber-junit" % "1.2.4" % "test",
  "junit" % "junit" % "4.12" % "test",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)

enablePlugins(CucumberPlugin)

CucumberPlugin.glue := ""