package org.hpi.esb.flink

import org.hpi.esb.commons.config.Configs.benchmarkConfig
import org.hpi.esb.commons.util.Logging

import sys.process._

class MultiJobRunner extends Logging {
  def runJob(queryName: String, inputTopic: String, outputTopic: String): Unit = {
    val flinkClusterPath = s"implementation/flink/cluster"
    val logFileName = s"${queryName}_${inputTopic}_$outputTopic.log"
    val flinkCommand = s"nohup flink run $flinkClusterPath/target/scala-2.11/Flink-Cluster-assembly-0.1.0-SNAPSHOT.jar" +
      s" -q $queryName -i $inputTopic -o $outputTopic &> $flinkClusterPath/log/$logFileName &"

    val command = Seq("bash", "-c", flinkCommand)
    logger.info(s"flink process status: ${command.!}")
  }
}

object MultiJobRunner {
  def main(args: Array[String]): Unit = {
    val runner = new MultiJobRunner
    val queryConfigs = benchmarkConfig.queryConfigs
    queryConfigs.foreach(q => runner.runJob(q.queryName, q.inputTopic, q.outputTopic))
  }
}
