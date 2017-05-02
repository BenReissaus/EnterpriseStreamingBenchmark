package org.hpi.esb.datasender.metrics

import org.scalatest.FunSpec
import org.scalatest.mockito.MockitoSugar
import org.hpi.esb.datasender.TestHelper.checkEquality


class SendMetricsTest extends FunSpec with MockitoSugar {

  val expectedRecordNumber = 1000
  val topics = List("A", "B")
  val sendMetrics = new SendMetrics(topics, expectedRecordNumber)

  describe("getFailedSendsPercentage") {

    it("should return 0 when expectedRecordNumber = 0") {
      val failedSends = 100
      val expectedRecordNumber = 0
      val failedSendsResult = SendResult(expectedRecordNumber, failedSends)
      assert(0.0 == failedSendsResult.failedSendsPercentage())
    }

    it("should calculate correctly") {
      val failedSends = 10
      val expectedRecordNumber = 100
      val failedSendsResult = SendResult(expectedRecordNumber, failedSends)
      assert(0.1 == failedSendsResult.failedSendsPercentage())
    }
  }

  describe("getAccumulatedSendMetrics") {
    val perTopicSendMetrics = Map("A" -> SendResult(expectedRecordNumber = 100L, failedSends = 10L),
      "B" -> SendResult(expectedRecordNumber = 100L, failedSends = 5L))

    val expectedAccumulatedSendMetrics = perTopicSendMetrics ++ Map("overall" -> SendResult(expectedRecordNumber = 200L, failedSends = 15L))
    val expectedRecordNumber = 100
    val accumulatedSendMetrics = sendMetrics.getAccumulatedSendResults(perTopicSendMetrics)

    it("should calculate the correct overall stats") {

      checkEquality[String, SendResult](expectedAccumulatedSendMetrics, accumulatedSendMetrics)
      checkEquality[String, SendResult](accumulatedSendMetrics, expectedAccumulatedSendMetrics)
    }
  }
}
