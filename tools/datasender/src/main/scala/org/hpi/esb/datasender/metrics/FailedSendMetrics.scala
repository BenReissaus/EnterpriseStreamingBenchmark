package org.hpi.esb.datasender.metrics

import org.hpi.esb.commons.output.ValueFormatter.roundPrecise
import org.hpi.esb.util.OffsetManagement

class FailedSendMetrics(topics: List[String], expectedRecordNumber: Long) extends DataSenderMetric {

  val failedSends = "failedSends"
  val failedPercentage = "failedPercentage"

  //returns Map of form ("topic" -> List("failedSends", "failedPercentage")
  override def getMetrics(): Map[String, List[String]] = {
    val sendMetrics = topics.map(topic =>
      topic -> getFailedSendMetric(topic, expectedRecordNumber)).toMap

    val accumulatedSendMetrics = getAccumulatedSendMetrics(sendMetrics, expectedRecordNumber)

    accumulatedSendMetrics.map { case (topic, (failedSends, failedSendsPercentage)) =>
      topic -> List(failedSends.toString, failedSendsPercentage.toString)
    }
  }

  def getFailedSendMetric(topic: String, expectedRecordNumber: Long): (Long, Double) = {
    val realRecordNumber = OffsetManagement.getNumberOfMessages(topic, partition = 0)
    val failedSends = expectedRecordNumber - realRecordNumber
    val failedPercentage = getFailedSendsPercentage(failedSends, expectedRecordNumber)
    (failedSends, failedPercentage)
  }

  def getFailedSendsPercentage(failedSends: Long, expectedRecordNumber: Long): Double = {
    val failedPercentage = if (expectedRecordNumber != 0) {
      BigDecimal(failedSends) / BigDecimal(expectedRecordNumber)
    } else {
      BigDecimal(0)
    }
    roundPrecise(failedPercentage, precision = 3)
  }

  def getAccumulatedSendMetrics(sendMetrics: Map[String, (Long, Double)],
                                expectedRecordNumber: Long): Map[String, (Long, Double)] = {

    val sumFailedSends = sendMetrics.foldLeft(0L) { case (acc, (_, (failedSends, _))) => acc + failedSends }
    val totalExpectedSends = expectedRecordNumber * sendMetrics.keys.toList.length
    val totalFailedSendsPercentage = getFailedSendsPercentage(sumFailedSends, totalExpectedSends)
    sendMetrics + ("overall" -> (sumFailedSends, totalFailedSendsPercentage))
  }

  override def getValueNames(): List[String] = List(failedSends, failedPercentage)
}
