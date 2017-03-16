package org.hpi.esb.datasender

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
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
  var mockedKafkaProducer: KafkaProducer[String, String] = mock[KafkaProducer[String, String]]
  var mockedDataReader: DataReader = mock[DataReader]

  describe("scale") {

    val record = List("dat0", "dat1", "dat2")

    it("should return an Option with a list of all three data record values when three topics are used") {
      val topics = List(topicA, topicB, topicC)
      val dataProducerThread = new DataProducerThread(mockedDataProducer,
        mockedKafkaProducer, mockedDataReader, topics)

      val scaledRecord = dataProducerThread.scale(Option(record)).get
      assert(scaledRecord.length == topics.length)
      assert(scaledRecord(0) == "dat0")
      assert(scaledRecord(1) == "dat1")
      assert(scaledRecord(2) == "dat2")
    }

    it("should return an Option with an empty list when an empty topics list is used") {
      val topics = List()
      val dataProducerThread = new DataProducerThread(mockedDataProducer,
        mockedKafkaProducer, mockedDataReader, topics)

      val scaledRecord = dataProducerThread.scale(Option(record)).get
      assert(scaledRecord.isEmpty)
    }

    it("should return an Option with an empty list when topics is set to 'None'") {
      val topics = List()
      val dataProducerThread = new DataProducerThread(mockedDataProducer,
        mockedKafkaProducer, mockedDataReader, topics)

      val scaledRecordOption = dataProducerThread.scale(Option(record))
      assert(scaledRecordOption.get.isEmpty)
    }

    it("should return all record values when too many topics are passed") {
      val topics = List(topicA, topicB, topicC, topicD)
      val dataProducerThread = new DataProducerThread(mockedDataProducer,
        mockedKafkaProducer, mockedDataReader, topics)

      val scaledRecord = dataProducerThread.scale(Option(record)).get
      assert(scaledRecord(0) == "dat0")
      assert(scaledRecord(1) == "dat1")
      assert(scaledRecord(2) == "dat2")
    }
  }

  describe("send") {
    val topics = List(topicA, topicB, topicC)
    val dataProducerThread = new DataProducerThread(mockedDataProducer,
      mockedKafkaProducer, mockedDataReader, topics)

    it("should send each record value to the corresponding kafka topic") {
      val scaledRecords = List("dat0", "dat1", "dat2")
      dataProducerThread.send(Option(scaledRecords))
      verify(mockedKafkaProducer, times(1)).send(new ProducerRecord[String, String](topics(0), scaledRecords(0)))
      verify(mockedKafkaProducer, times(1)).send(new ProducerRecord[String, String](topics(1), scaledRecords(1)))
      verify(mockedKafkaProducer, times(1)).send(new ProducerRecord[String, String](topics(2), scaledRecords(2)))
    }

    it("should not send anything to kafka when no record values are passed") {
      val mockedKafkaProducer = mock[KafkaProducer[String, String]]
      val dataProducerThread = new DataProducerThread(mockedDataProducer,
        mockedKafkaProducer, mockedDataReader, topics)

      dataProducerThread.send(None)
      verify(mockedKafkaProducer, never()).send(ArgumentMatchers.any[ProducerRecord[String, String]])
    }
  }

  describe("run") {

    val topics = List(topicA, topicB, topicC)
    val columns = List("timestamp", "id", "stream0", "stream1", "stream2")
    val source: Source = Source.fromString(
      """ts id dat00 dat01 dat02
        |ts id dat10 dat11 dat12
      """.stripMargin)
    val dataReader = new DataReader(source, columns, columnDelimiter = " ", dataColumnStart = 2)

    val dataProducerThread = new DataProducerThread(mockedDataProducer,
      mockedKafkaProducer, dataReader, topics)

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
        mockedKafkaProducer, dataReader, topics)

      dataProducerThread.run()
      verify(mockedKafkaProducer, never()).send(ArgumentMatchers.any[ProducerRecord[String, String]])
      verify(mockedDataProducer, times(1)).shutDown()
    }
  }
}
