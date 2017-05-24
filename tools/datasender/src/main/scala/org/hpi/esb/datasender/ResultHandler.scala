package org.hpi.esb.datasender

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.kafka.clients.producer.KafkaProducer
import org.hpi.esb.commons.config.Configs.BenchmarkConfig
import org.hpi.esb.commons.output.{CSVOutput, Tabulator}
import org.hpi.esb.commons.util.Logging
import org.hpi.esb.datasender.config._
import org.hpi.esb.datasender.metrics.MetricHandler

class ResultHandler(config: Config, benchmarkConfig: BenchmarkConfig,
                    kafkaProducer: KafkaProducer[String, String]) extends Logging {

  val resultsPath = s"${ConfigHandler.dataSenderPath}/results"
  val currentTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
  val resultFileName = s"${currentTime}_prefix_${benchmarkConfig.topicPrefix}_postfix_${benchmarkConfig.topicPostfix}" +
    s"_scale_${benchmarkConfig.scaleFactor}.csv"

  def outputResults(topicOffsets: Map[String, Long], expectedRecordNumber: Int): Unit = {
    val metricHandler = new MetricHandler(kafkaProducer, topicOffsets, expectedRecordNumber)
    val metrics = metricHandler.fetchMetrics()
    val (metricNames, metricValues) = getSortedNameValueLists(metrics)

    val importantConfigs = ConfigHandler.getImportantConfigValues(config)
    val (configNames, configValues) = getSortedNameValueLists(importantConfigs)

    val table = List(configNames ++ metricNames, configValues ++ metricValues)
    CSVOutput.write(table, resultsPath, resultFileName)
    logger.info(Tabulator.format(table))
  }

  def getSortedNameValueLists(m: Map[String, String]): (List[String], List[String]) = {
    m.map { case (key, value) => (key, value) }.toList
      .sortWith(_._1 < _._1)
      .unzip
  }
}
