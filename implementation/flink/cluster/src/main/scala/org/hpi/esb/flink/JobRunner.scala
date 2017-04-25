package org.hpi.esb.flink

import org.hpi.esb.commons.config.Configs.QueryConfig

object JobRunner {
  def main(args: Array[String]): Unit = {
    val config = getConfig(args)
    val job = new QueryJob(config.queryName, config.inputTopic, config.outputTopic)
    job.execute()
  }

  def getConfig(args: Array[String]): QueryConfig = {
    val parser = new scopt.OptionParser[QueryConfig]("FlinkJob") {
      head("Apache Flink Implementation of 'Enterprise Streaming Benchmark'")

      opt[String]('q', "queryName")
        .required()
        .action((arg, c) => c.copy(queryName = arg))
        .text("Name of the Query to execute.")

      opt[String]('i', "inputTopic")
        .required()
        .action((arg, c) => c.copy(inputTopic = arg))
        .text("Name of the input topic.")

      opt[String]('o', "outputTopic")
        .required()
        .action((arg, c) => c.copy(outputTopic = arg))
        .text("Name of the output topic.")
      help("help")
    }

    parser.parse(args, QueryConfig()) match {
      case Some(c) => c
      case _ => sys.exit(1)
    }
  }
}
