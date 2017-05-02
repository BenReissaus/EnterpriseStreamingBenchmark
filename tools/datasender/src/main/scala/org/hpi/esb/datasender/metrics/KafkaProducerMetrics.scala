package org.hpi.esb.datasender.metrics

import org.apache.kafka.clients.producer.KafkaProducer
import org.hpi.esb.commons.output.ValueFormatter.round

import scala.collection.JavaConversions._

class KafkaProducerMetrics(kafkaProducer: KafkaProducer[String, String]) extends Metric {
  val recordSendRateMetric = "record-send-rate"

  def getMetrics(): Map[String, List[String]] = filterMetric(recordSendRateMetric)
  def getValueNames(): List[String] = List(recordSendRateMetric)

  /**
    * filter the desired metric from all available kafka producer metrics
    * per topic and overall
    *
    * @param name
    * @return
    */
  def filterMetric(name: String): Map[String, List[String]] = {
    val accMetrics = Map[String, String]()
    val filteredMetris = kafkaProducer.metrics().foldLeft(accMetrics) {
      case (acc, (metricName, metric)) if metricName.name() == name => {
        val key = if (metricName.group() == "producer-metrics") "overall" else metricName.tags().get("topic")
        val value = round(metric.value(), precision = 2)
        val newAcc = acc ++ Map[String, String](key -> value.toString)
        newAcc
      }
      case (acc, _) => acc
    }

    filteredMetris.map { case (key, value) => key -> List(value) }
  }

}
