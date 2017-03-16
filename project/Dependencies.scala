import sbt._

object Dependencies {

  val testDependencies = Seq(
    "org.scalamock" %% "scalamock-scalatest-support" % "3.4.2" % Test,
    "org.mockito" % "mockito-core" % "2.6.3" % Test,
    "org.scalactic" % "scalactic_2.11" % "3.0.1" % Test,
    "org.scalatest" % "scalatest_2.11" % "3.0.1" % Test
  )

  val kafkaClients = Seq(
    "org.apache.kafka" % "kafka-clients" % "0.10.1.0"
  )

  val kafka = Seq(
    "org.apache.kafka" % "kafka_2.11" % "0.10.1.0"
  )

  val loggingDependencies = Seq(
    "log4j" % "log4j" % "1.2.14"
  )

  val scalaIODependencies = Seq(
    "com.github.scala-incubator.io" % "scala-io-core_2.11" % "0.4.3-1",
    "com.github.scala-incubator.io" % "scala-io-file_2.11" % "0.4.3-1"
  )

  val configHandlingDependency = Seq(
    "com.github.scopt" %% "scopt" % "3.5.0",
    "com.github.melrief" %% "pureconfig" % "0.5.0"
  )

  def flinkDependencies(flinkVersion: String): Seq[ModuleID] = Seq(
    "org.apache.flink" %% "flink-scala" % flinkVersion,
    "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
    "org.apache.flink" %% "flink-connector-kafka-0.10" % flinkVersion
  )

}
