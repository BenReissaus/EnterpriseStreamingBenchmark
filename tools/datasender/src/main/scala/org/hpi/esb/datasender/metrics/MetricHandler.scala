package org.hpi.esb.datasender.metrics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.kafka.clients.producer.KafkaProducer
import org.hpi.esb.commons.output.{CSVOutput, Tabulator}
import org.hpi.esb.commons.util.Logging
import org.hpi.esb.config.ConfigHandler

class MetricHandler(kafkaProducer: KafkaProducer[String, String], topics: List[String], scaleFactor: String,
                    ack: String, batchSize: String, sendingInterval: String, expectedRecordNumber: Long) extends Logging {

  val resultsPath = s"${ConfigHandler.dataSenderPath}/results"
  val currentTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
  val resultFileName = s"${currentTime}_scale_${scaleFactor}_ack_${ack}_batch_${batchSize}_interval_${sendingInterval}.csv"

  def execute(): Unit = {

    val table = fetch()
    CSVOutput.write(table, resultsPath, resultFileName)
    logger.info(Tabulator.format(table))
  }

  def fetch(): List[List[String]] = {
    val kafkaProducerMetrics = new KafkaProducerMetrics(kafkaProducer)
    val kafkaProducerMetricsValues = kafkaProducerMetrics.getMetrics()
    val sendMetrics = new FailedSendMetrics(topics, expectedRecordNumber)
    val sendMetricsValues = sendMetrics.getMetrics()

    val mergedValues = merge(kafkaProducerMetricsValues, sendMetricsValues)
    val header = List("topic") ++ kafkaProducerMetrics.getValueNames() ++ sendMetrics.getValueNames()

    val rows = mergedValues
      .map { case (key, value) => key :: value }.toList
      .sortWith(_.head < _.head)

    header :: rows
  }

  def merge(m1: Map[String, List[String]], m2: Map[String, List[String]]): Map[String, List[String]] = {
    val merged = m1.toSeq ++ m2.toSeq
    val grouped = merged.groupBy(_._1)

    // value is list of lists
    val accumulatedValues = grouped.mapValues(_.map(_._2).toList)

    // merge list of lists of values
    accumulatedValues.map {
      case (key, value) =>
        key -> value.foldLeft(List.empty[String])((a, b) => a ++ b)
    }
  }
}
