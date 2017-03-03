import Dependencies._

name := "EnterpriseStreamingBenchmark"

lazy val commonSettings = Seq(
  organization := "org.hpi",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.11.8"
)
val flinkVersion = "1.2.0"

lazy val root = (project in file(".")).settings(commonSettings).aggregate(datasender, validator, flinkCluster)

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
    mainClass in Compile := Some("org.hpi.esb.datasender.Main")
  )

lazy val validator = (project in file("tools/validator")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= kafkaClients,
    libraryDependencies ++= loggingDependencies,
    libraryDependencies ++= configHandlingDependency,
    libraryDependencies ++= testDependencies
  ).
  settings(
    name := "Validator",
    mainClass in Compile := Some("org.hpi.esb.datavalidator.Main")
  )

lazy val flinkCluster = (project in file("implementation/flink/application")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= testDependencies,
    libraryDependencies ++= flinkDependencies(flinkVersion)
  ).
  settings(
    name := "Flink",
    mainClass in (Compile,run) := Some("org.hpi.esb.flink.Main"),
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
  )

lazy val flinkLocal = (project in file("implementation/flink/intellij")).dependsOn(flinkCluster).
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
    mainClass in (Compile,run) := Some("org.hpi.esb.flink.Main"),
    // make run command include the provided dependencies
    run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))
  )

