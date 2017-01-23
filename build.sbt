resolvers in ThisBuild ++= Seq("Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/", Resolver.mavenLocal)
scalaVersion in ThisBuild := "2.11.8"

val flinkVersion = "1.2-SNAPSHOT"
val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-connector-kafka-0.10" % flinkVersion
)

val testDependencies = Seq(
  "org.scalactic" % "scalactic_2.11" % "3.0.1",
  "org.scalatest" % "scalatest_2.11" % "3.0.1" % "test"
)

lazy val root = (project in file(".")).
  settings(
    name := "ESB-Flink",
    version := "0.1-SNAPSHOT",
    organization := "org.hpi",
    libraryDependencies ++= (flinkDependencies ++ testDependencies)
  )

mainClass in (Compile,run) := Some("org.hpi.esb.flink.Main")

// make run command include the provided dependencies
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

//// exclude Scala library from assembly
//assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
