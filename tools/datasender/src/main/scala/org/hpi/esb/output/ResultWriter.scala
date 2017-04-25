package org.hpi.esb.output

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.hpi.esb.commons.output.ValueFormatter.round
import org.hpi.esb.commons.output.{CSVOutput, Tabulator}
import org.hpi.esb.commons.util.Logging
import org.hpi.esb.config.ConfigHandler

import scala.collection.JavaConversions._

class ResultWriter(kafkaProducer: KafkaProducer[String, String], scaleFactor: String, ack: String, batchSize: String, sendingInterval: String) extends Logging {

  val resultsPath = s"${ConfigHandler.dataSenderPath}/results"
  val currentTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
  val resultFileName = s"${currentTime}_scale_${scaleFactor}_ack_${ack}_batch_${batchSize}_interval_${sendingInterval}.csv"

  def write(): Unit = {
    val recordSendRateMetric = "record-send-rate"
    val accMetrics = Map[String, String]()
    val filteredMetrics = kafkaProducer.metrics().foldLeft(accMetrics) {
      case (acc, (metricName, metric)) if metricName.name() == recordSendRateMetric => {
        val key = if (metricName.group() == "producer-metrics") "overall" else metricName.tags().get("topic")
        val value = round(metric.value(), precision = 2)
        val newAcc = acc ++ Map[String, String](key -> value.toString)
        newAcc
      }
      case (acc, _) => acc
    }

    val header = List(recordSendRateMetric, "value (records/s)")
    val values = filteredMetrics
      .map { case (key, value) => List(key, value) }.toList
      .sortWith(_.head < _.head)

    val table = header :: values
    CSVOutput.write(table, resultsPath, resultFileName)
    logger.info(Tabulator.format(table))
  }
}
