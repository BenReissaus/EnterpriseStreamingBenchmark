package org.hpi.esb.datasender.metrics

import org.apache.kafka.clients.producer.KafkaProducer
import org.scalatest.{FunSpec, FunSuite}
import org.scalatest.mockito.MockitoSugar
import org.hpi.esb.datasender.TestHelper.checkEquality

class MetricHandlerTest extends FunSpec with MockitoSugar {

  val topics = List("t1", "t2")
  val scaleFactor = "1"
  val ack = "1"
  val batchSize = "1000"
  val sendingInterval = "10"
  val expectedRecordNumber = 100
  val metricHandler = new MetricHandler(mock[KafkaProducer[String, String]], topics,
    scaleFactor, ack, batchSize, sendingInterval, expectedRecordNumber)

  describe("merge") {

    it("should merge the values of two maps with the same keys correctly") {
      val m1 = Map("1" -> List("A", "B"))
      val m2 = Map("1" -> List("C", "D"))
      val expected = Map("1" -> List("A", "B", "C", "D"))

      checkEquality[String, List[String]](metricHandler.merge(m1, m2), expected)
    }

  }

}
