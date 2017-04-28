package org.hpi.esb.datasender

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{never, times, verify}
import org.scalatest._
import org.scalatest.mockito.MockitoSugar

import scala.io.Source


class DataProducerThreadTest extends FunSpec with MockitoSugar {

  val topicA = "ESB_A_1"
  val topicB = "ESB_B_1"
  val topicC = "ESB_C_1"
  val topicD = "ESB_D_1"
  var mockedDataProducer: DataProducer = mock[DataProducer]
  var mockedDataReader: DataReader = mock[DataReader]

  describe("send - multi column mode") {
    val topics = List(topicA, topicB, topicC)
    val singleColumnMode = false
    var mockedKafkaProducer: KafkaProducer[String, String] = mock[KafkaProducer[String, String]]
    val dataProducerThread = new DataProducerThread(mockedDataProducer,
      mockedKafkaProducer, mockedDataReader, topics, singleColumnMode)

    it("should send each record value to the corresponding kafka topic") {
      val records = List("dat0", "dat1", "dat2")
      dataProducerThread.send(Option(records))
      verify(mockedKafkaProducer, times(1)).send(new ProducerRecord[String, String](topics(0), records(0)))
      verify(mockedKafkaProducer, times(1)).send(new ProducerRecord[String, String](topics(1), records(1)))
      verify(mockedKafkaProducer, times(1)).send(new ProducerRecord[String, String](topics(2), records(2)))
    }

    it("should not send anything to kafka when no record values are passed") {
      val mockedKafkaProducer = mock[KafkaProducer[String, String]]
      val dataProducerThread = new DataProducerThread(mockedDataProducer,
        mockedKafkaProducer, mockedDataReader, topics, singleColumnMode)

      dataProducerThread.send(None)
      verify(mockedKafkaProducer, never()).send(ArgumentMatchers.any(), ArgumentMatchers.any())
    }
  }

  describe("send - single column mode") {
    val topics = List(topicA, topicB, topicC)
    val singleColumnMode = true
    var mockedKafkaProducer: KafkaProducer[String, String] = mock[KafkaProducer[String, String]]
    val dataProducerThread = new DataProducerThread(mockedDataProducer,
      mockedKafkaProducer, mockedDataReader, topics, singleColumnMode)

    it("should send the same record value to the corresponding kafka topic") {
      val records = List("dat0", "dat1", "dat2")
      dataProducerThread.send(Option(records))
      verify(mockedKafkaProducer, times(1)).send(new ProducerRecord[String, String](topics(0), records(0)))
      verify(mockedKafkaProducer, times(1)).send(new ProducerRecord[String, String](topics(1), records(0)))
      verify(mockedKafkaProducer, times(1)).send(new ProducerRecord[String, String](topics(2), records(0)))
    }

    it("should not send anything to kafka when no record values are passed") {
      val mockedKafkaProducer = mock[KafkaProducer[String, String]]
      val dataProducerThread = new DataProducerThread(mockedDataProducer,
        mockedKafkaProducer, mockedDataReader, topics, singleColumnMode)

      dataProducerThread.send(None)
      verify(mockedKafkaProducer, never()).send(ArgumentMatchers.any(), ArgumentMatchers.any())
    }
  }

  describe("run") {

    val topics = List(topicA, topicB, topicC)
    val singleColumnMode = false
    val columns = List("timestamp", "id", "stream0", "stream1", "stream2")
    val source: Source = Source.fromString(
      """ts id dat00 dat01 dat02
        |ts id dat10 dat11 dat12
      """.stripMargin)
    val dataReader = new DataReader(source, columns, columnDelimiter = " ", dataColumnStart = 2)

    var mockedKafkaProducer: KafkaProducer[String, String] = mock[KafkaProducer[String, String]]
    val dataProducerThread = new DataProducerThread(mockedDataProducer,
      mockedKafkaProducer, dataReader, topics, singleColumnMode)

    it("should send records as long as there are records left") {
      dataProducerThread.run()
      verify(mockedKafkaProducer, times(1)).send(new ProducerRecord[String, String](topicA, "dat00"))
      verify(mockedKafkaProducer, times(1)).send(new ProducerRecord[String, String](topicB, "dat01"))
      verify(mockedKafkaProducer, times(1)).send(new ProducerRecord[String, String](topicC, "dat02"))
    }

    it("should not send records when there are no records left and shutdown") {
      val mockedKafkaProducer = mock[KafkaProducer[String, String]]
      val source: Source = Source.fromString("")
      val dataReader = new DataReader(source, columns, columnDelimiter = " ", dataColumnStart = 2)
      val dataProducerThread = new DataProducerThread(mockedDataProducer,
        mockedKafkaProducer, dataReader, topics, singleColumnMode)

      dataProducerThread.run()
      verify(mockedKafkaProducer, never()).send(ArgumentMatchers.any[ProducerRecord[String, String]])
      verify(mockedDataProducer, times(1)).shutDown()
    }
  }
}
