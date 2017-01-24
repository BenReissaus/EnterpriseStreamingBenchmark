libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "0.10.1.0",
  "org.slf4j" % "slf4j-jdk14" % "1.7.5",
  "log4j" % "log4j" % "1.2.14",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "org.scalamock" %% "scalamock-scalatest-support" % "3.4.2" % Test,
  "org.mockito" % "mockito-core" % "2.6.3",
	"com.github.scala-incubator.io" % "scala-io-core_2.11" % "0.4.3-1",
	"com.github.scala-incubator.io" % "scala-io-file_2.11" % "0.4.3-1",
	"com.github.melrief" %% "pureconfig" % "0.5.0"
)

lazy val root = (project in file("."))
  .settings(
      name := "DataSender",
      organization := "org.hpi",
      scalaVersion := "2.11.8",
      version := "1.0",
      mainClass in Compile := Some("org.hpi.esb.datasender.Main")
    )
