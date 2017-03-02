libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "0.10.1.0",
  "org.slf4j" % "slf4j-jdk14" % "1.7.5",
  "log4j" % "log4j" % "1.2.14",
  "org.scalactic" % "scalactic_2.11" % "3.0.1",
  "org.scalatest" % "scalatest_2.11" % "3.0.1" % "test",
  "com.github.melrief" %% "pureconfig" % "0.5.0"
)

lazy val root = (project in file("."))
  .settings(
    name := "ESB-DataValidator",
    organization := "org.hpi",
    scalaVersion := "2.11.8",
    version := "1.0",
    mainClass in Compile := Some("org.hpi.esb.datavalidator.Main")
  )
