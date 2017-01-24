package org.hpi.esb.datasender

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.mockito.Mockito.{times, verify, when}
import org.scalatest._
import org.scalatest.mockito.MockitoSugar

class DataProducerThreadTest extends FlatSpec with Matchers with PrivateMethodTester with BeforeAndAfter with MockitoSugar {

  val topic: String = "FC Magdeburg"
  val record: String = "10 11 12 13 14 15"
  val alternativeDelimiter: String = ","
  val recordWithAlternativeDelimiter: String = s"10${alternativeDelimiter}11${alternativeDelimiter}12${alternativeDelimiter}13"
  val defaultColumnDelimiter: String = "s+" //any number of whitespaces
  val columnToBeSend: Int = -1
  var mockedKafkaProducer: KafkaProducer[String, String] = null
  var mockedDataReader: DataReader = null
  var mockedDataProducer: DataProducer = null

  before {
  }

  after {
  }

  def initializeDefaultDataProducerThread(column: Int = columnToBeSend, columnDelimiter: String = defaultColumnDelimiter): DataProducerThread = {
    mockedKafkaProducer = mock[KafkaProducer[String, String]]
    mockedDataReader = mock[DataReader]
    mockedDataProducer = mock[DataProducer]
    new DataProducerThread(mockedDataProducer, mockedKafkaProducer, mockedDataReader, topic, column, columnDelimiter)
  }

  "A DataProducerThread" should "return the specified column (idx 1)" in {
    val dataProducerThread = initializeDefaultDataProducerThread(1)
    when(mockedDataReader.getLine).thenReturn(record)
    dataProducerThread.run
    verify(mockedKafkaProducer, times(1)).send(new ProducerRecord[String, String](topic, "11"))
  }

  it should "return the specified column with column delimiter that is not default" in {
    val dataProducerThread = initializeDefaultDataProducerThread(2, alternativeDelimiter)
    when(mockedDataReader.getLine).thenReturn(recordWithAlternativeDelimiter)
    dataProducerThread.run
    verify(mockedKafkaProducer, times(1)).send(new ProducerRecord[String, String](topic, "12"))
  }

  it should "return a whole data record if specified, i.e. columnToSend < 0" in {
    val dataProducerThread = initializeDefaultDataProducerThread()
    when(mockedDataReader.getLine).thenReturn(record)
    dataProducerThread.run
    verify(mockedKafkaProducer, times(1)).send(new ProducerRecord[String, String](topic, record))
  }

  it should "does not send a message and shutdown DataProducer if data source is empty / end is reached" in {
    val dataProducerThread = initializeDefaultDataProducerThread()
    when(mockedDataReader.getLine).thenReturn(null)
    dataProducerThread.run
    verify(mockedKafkaProducer, times(0)).send(org.mockito.ArgumentMatchers.any[ProducerRecord[String, String]])
    verify(mockedDataProducer, times(1)).shutDown
  }
}