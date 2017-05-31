package org.hpi.esb.datasender.output

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.kafka.clients.producer.KafkaProducer
import org.hpi.esb.commons.config.Configs.BenchmarkConfig
import org.hpi.esb.commons.output.{CSVOutput, Tabulator}
import org.hpi.esb.commons.util.Logging
import org.hpi.esb.datasender.config._
import org.hpi.esb.datasender.metrics.MetricHandler
import org.hpi.esb.datasender.output.OutputHelper._

class BenchmarkRunResultWriter(config: Config, benchmarkConfig: BenchmarkConfig,
                               kafkaProducer: KafkaProducer[String, String]) extends Logging {

  val currentTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())

  def outputResults(topicOffsets: Map[String, Long], expectedRecordNumber: Int): Unit = {
    val metricHandler = new MetricHandler(kafkaProducer, topicOffsets, expectedRecordNumber)
    val metrics = metricHandler.fetchMetrics()
    val (metricNames, metricValues) = getSortedKeyValueLists(metrics)

    val importantConfigs = ConfigHandler.getImportantConfigValues(config)
    val (configNames, configValues) = getSortedKeyValueLists(importantConfigs)

    val table = List(configNames ++ metricNames, configValues ++ metricValues)
    CSVOutput.write(table, ConfigHandler.resultsPath, ConfigHandler.resultFileName(currentTime))
    logger.info(Tabulator.format(table))
  }

}
