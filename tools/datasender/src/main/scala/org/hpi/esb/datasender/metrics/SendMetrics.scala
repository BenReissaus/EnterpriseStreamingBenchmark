package org.hpi.esb.datasender.metrics

import org.hpi.esb.commons.output.ValueFormatter.roundPrecise
import org.hpi.esb.util.OffsetManagement

case class SendResult(expectedRecordNumber: Long = 0, failedSends: Long = 0) {
  def failedSendsPercentage(): Double = {
    val failedPercentage = if (expectedRecordNumber != 0) {
      BigDecimal(failedSends) / BigDecimal(expectedRecordNumber)
    } else {
      BigDecimal(0)
    }
    roundPrecise(failedPercentage, precision = 3)
  }

  def update(expectedRecordNumber: Long, failedSends: Long): SendResult = {
    val sumExpectedRecordNumber = this.expectedRecordNumber + expectedRecordNumber
    val sumFailedSends = this.failedSends + failedSends
    SendResult(sumExpectedRecordNumber, sumFailedSends)
  }
}
class SendMetrics(topics: List[String], expectedTopicNumber: Long) extends Metric {

  val expectedRecordNumberName = "expectedRecordNumber"
  val failedSendsName = "failedSends"
  val failedPercentageName = "failedPercentage"

  //returns Map of form ("topic" -> List("expectedRecordNumber", "failedSends", "failedPercentage")
  override def getMetrics(): Map[String, List[String]] = {
    val sendResults = topics.map(topic =>
      topic -> getSendResultForTopic(topic, expectedTopicNumber)).toMap

    val accumulatedSendResults = getAccumulatedSendResults(sendResults)

    accumulatedSendResults.map { case (topic, sendResult) =>
      topic -> List(sendResult.expectedRecordNumber.toString,
        sendResult.failedSends.toString,
        sendResult.failedSendsPercentage().toString)
    }
  }

  def getSendResultForTopic(topic: String, expectedRecordNumber: Long): SendResult = {
    val realRecordNumber = OffsetManagement.getNumberOfMessages(topic, partition = 0)
    val failedSends = expectedRecordNumber - realRecordNumber
    SendResult(expectedRecordNumber = expectedRecordNumber, failedSends = failedSends)
  }

  def getAccumulatedSendResults(sendMetrics: Map[String, SendResult]): Map[String, SendResult] = {

    val overallSendResults = sendMetrics.foldLeft(SendResult()) {
      case (acc, (_, SendResult(expectedRecordNumber, failedSends))) => acc.update(expectedRecordNumber, failedSends) }

    sendMetrics + ("overall" -> overallSendResults)
  }

  override def getValueNames(): List[String] = List(expectedRecordNumberName, failedSendsName, failedPercentageName)
}
