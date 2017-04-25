import Dependencies._

name := "EnterpriseStreamingBenchmark"

lazy val commonSettings = Seq(
  organization := "org.hpi",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.11.8"
)
val flinkVersion = "1.2.0"

lazy val root = (project in file(".")).
  settings(commonSettings).
  aggregate(datasender, validator, flinkCluster, util)

lazy val commons = (project in file("tools/commons")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= configHandlingDependency,
    libraryDependencies ++= scalaIODependencies,
    libraryDependencies ++= loggingDependencies
  ).
  settings(
    name := "Commons"
  )

lazy val datasender = (project in file("tools/datasender")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= kafkaClients,
    libraryDependencies ++= loggingDependencies,
    libraryDependencies ++= scalaIODependencies,
    libraryDependencies ++= configHandlingDependency,
    libraryDependencies ++= testDependencies
  ).
  settings(
    name := "DataSender",
    mainClass in assembly := Some("org.hpi.esb.datasender.Main")
  ).
  dependsOn(commons)

lazy val validator = (project in file("tools/validator")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= loggingDependencies,
    libraryDependencies ++= configHandlingDependency,
    libraryDependencies ++= testDependencies,
    libraryDependencies ++= metrics,
    libraryDependencies ++= akka,
    libraryDependencies ++= csv
  ).
  settings(
    name := "Validator",
    mainClass in Compile := Some("org.hpi.esb.datavalidator.Main")
  ).
  dependsOn(commons, util)

lazy val util = (project in file("tools/util")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= kafka,
    libraryDependencies ++= configHandlingDependency,
    libraryDependencies ++= testDependencies
  ).
  settings(
    name := "Util",
    mainClass in (Compile,run) := Some("org.hpi.esb.util.Main")
  ).
  dependsOn(commons)

lazy val flinkCluster = (project in file("implementation/flink/cluster")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= testDependencies,
    libraryDependencies ++= flinkDependencies(flinkVersion)
  ).
  settings(
    name := "Flink-Cluster",
    mainClass in (Compile,run) := Some("org.hpi.esb.flink.JobRunner"),
    mainClass in assembly := Some("org.hpi.esb.flink.JobRunner"),
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
  ).
  dependsOn(commons)

lazy val flinkLocal = (project in file("implementation/flink/local")).dependsOn(flinkCluster).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= flinkDependencies(flinkVersion).map {
      module =>
        if (module.configurations.equals(Some("provided"))) {
          module.copy(configurations = None)
        } else {
          module
        }
    }
  ).
  settings(
    name := "Flink-Local",
    mainClass in (Compile,run) := Some("org.hpi.esb.flink.JobRunner"),
    mainClass in assembly := Some("org.hpi.esb.flink.JobRunner"),
    // make run command include the provided dependencies
    run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))
  ).
  dependsOn(commons)

