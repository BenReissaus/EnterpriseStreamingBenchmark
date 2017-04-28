package org.hpi.esb.datasender.metrics

abstract class DataSenderMetric {
  def getMetrics(): Map[String, List[String]]
  def getValueNames(): List[String]
}
