package org.hpi.esb.datasender.metrics

import org.scalatest.FunSpec
import org.scalatest.mockito.MockitoSugar
import org.hpi.esb.datasender.TestHelper.checkEquality


class FailedSendMetricsTest extends FunSpec with MockitoSugar {

  val expectedRecordNumber = 1000
  val topics = List("A", "B")
  val sendMetrics = new FailedSendMetrics(topics, expectedRecordNumber)

  describe("getFailedSendsPercentage") {

    it("should return 0 when expectedRecordNumber = 0") {
      val failedSends = 100
      val expectedRecordNumber = 0
      assert(0.0 == sendMetrics.getFailedSendsPercentage(failedSends, expectedRecordNumber))
    }

    it("should calculate correctly") {
      val failedSends = 10
      val expectedRecordNumber = 100
      assert(0.1 == sendMetrics.getFailedSendsPercentage(failedSends, expectedRecordNumber))
    }
  }

  describe("getAccumulatedSendMetrics") {
    val perTopicSendMetrics = Map("A" -> (10L, 0.1), "B" -> (5L, 0.05))
    val expectedAccumulatedSendMetrics = perTopicSendMetrics ++ Map("overall" -> (15L, 0.075))
    val expectedRecordNumber = 100
    val accumulatedSendMetrics = sendMetrics.getAccumulatedSendMetrics(perTopicSendMetrics, expectedRecordNumber)

    it("should calculate the correct overall stats") {

      checkEquality[String, (Long, Double)](expectedAccumulatedSendMetrics, accumulatedSendMetrics)
      checkEquality[String, (Long, Double)](accumulatedSendMetrics, expectedAccumulatedSendMetrics)
    }
  }
}
