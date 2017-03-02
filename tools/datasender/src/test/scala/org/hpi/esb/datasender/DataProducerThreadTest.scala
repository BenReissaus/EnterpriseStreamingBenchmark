package org.hpi.esb.datasender

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.Logger
import org.mockito.Mockito.{times, verify, when}
import org.scalatest._
import org.scalatest.mockito.MockitoSugar

class DataProducerThreadTest extends FlatSpec with Matchers with PrivateMethodTester with BeforeAndAfter with MockitoSugar {

  val topicOptionNotDefined: Option[String] = None: Option[String]
  val defaultTopicList: List[String] = List("1", ".", "F", "C", "Magdeburg", "FCM")
  val defaultColumnDelimiter: String = "s+" //any number of whitespaces
  val alternativeDelimiter: String = ","
  val defaultRecord: String = "10 11 12 13 14 15"
  val recordAsValueArray: Array[String] = defaultRecord.split(s"\\$defaultColumnDelimiter")
  val recordWithAlternativeDelimiter: String =
    s"10${alternativeDelimiter}11${alternativeDelimiter}12${alternativeDelimiter}13${alternativeDelimiter}14${alternativeDelimiter}15"
  val recordWithAlternativeDelimiterAsValueList: Array[String] = recordWithAlternativeDelimiter.split(s"\\$alternativeDelimiter")
  val defaultColumnStartOption: Option[Int] = Some(2)
  val defaultColumnEndOption: Option[Int] = Some(3)
  val columnOptionNull: Option[Int] = None: Option[Int]
  var mockedKafkaProducer: KafkaProducer[String, String] = _
  var mockedDataReader: DataReader = _
  var mockedDataProducer: DataProducer = _

  before {
  }

  after {
  }

  def initializeDefaultDataProducerThread(columnList: List[String] = defaultTopicList,
                                          columnDelimiter: String = defaultColumnDelimiter,
                                          columnStartOption: Option[Int] = defaultColumnStartOption,
                                          columnEndOption: Option[Int] = defaultColumnEndOption): DataProducerThread = {
    mockedKafkaProducer = mock[KafkaProducer[String, String]]
    mockedDataReader = mock[DataReader]
    mockedDataProducer = mock[DataProducer]
    new DataProducerThread(
      dataProducer = mockedDataProducer,
      kafkaProducer = mockedKafkaProducer,
      dataReader = mockedDataReader,
      topicList = columnList,
      columnDelimiter = columnDelimiter,
      columnStartOption = columnStartOption,
      columnEndOption = columnEndOption)
  }

  "A DataProducerThread" should "return the specified column (idx 1)" in {
    val idx = 1
    val dataProducerThread = initializeDefaultDataProducerThread(columnStartOption = Some(idx), columnEndOption = Some(idx))
    when(mockedDataReader.getLine).thenReturn(Some(defaultRecord))
    dataProducerThread.run()
    verify(mockedKafkaProducer, times(1)).send(new ProducerRecord[String, String](defaultTopicList(idx), recordAsValueArray(idx)))
  }

  it should "return the specified column with column delimiter that is not default" in {
    val idx = 2
    val dataProducerThread = initializeDefaultDataProducerThread(columnStartOption = Some(idx), columnEndOption =
      Some(idx), columnDelimiter = alternativeDelimiter)
    when(mockedDataReader.getLine).thenReturn(Some(recordWithAlternativeDelimiter))
    dataProducerThread.run()
    verify(mockedKafkaProducer, times(1)).send(new ProducerRecord[String, String](defaultTopicList(idx), recordAsValueArray(idx)))
  }

  it should "return a whole data record if specified, i.e. column list contains one entry" in {
    val topic = "FCM"
    val dataProducerThread = initializeDefaultDataProducerThread(columnList = List(topic), columnStartOption = None, columnEndOption = None)
    when(mockedDataReader.getLine).thenReturn(Some(defaultRecord))
    dataProducerThread.run()
    verify(mockedKafkaProducer, times(1)).send(new ProducerRecord[String, String](topic, defaultRecord))
  }

  it should "not send a message and shutdown DataProducer if data source is empty / end is reached" in {
    val dataProducerThread = initializeDefaultDataProducerThread()
    when(mockedDataReader.getLine).thenReturn(None: Option[String])
    dataProducerThread.run()
    verify(mockedKafkaProducer, times(0)).send(org.mockito.ArgumentMatchers.any[ProducerRecord[String, String]])
    verify(mockedDataProducer, times(1)).shutDown()
  }

  it should "send correct messages to correct topics when sending each value individually" in {
    val dataProducerThread = initializeDefaultDataProducerThread(columnList = defaultTopicList, columnStartOption =
      columnOptionNull, columnEndOption = columnOptionNull)
    when(mockedDataReader.getLine).thenReturn(Some(defaultRecord))
    dataProducerThread.run()
    var idx: Int = 0
    for (topic <- defaultTopicList) {
      verify(mockedKafkaProducer, times(1)).send(new ProducerRecord[String, String](topic, recordAsValueArray(idx)))
      idx += 1
    }
  }

  it should "send correct messages to correct topics when sending each value individually - not-default column delimiter" in {
    val dataProducerThread = initializeDefaultDataProducerThread(columnList = defaultTopicList, columnStartOption = columnOptionNull, columnEndOption =
      columnOptionNull, columnDelimiter = alternativeDelimiter)
    when(mockedDataReader.getLine).thenReturn(Some(recordWithAlternativeDelimiter))
    dataProducerThread.run()
    var idx: Int = 0
    for (topic <- defaultTopicList) {
      verify(mockedKafkaProducer, times(1)).send(new ProducerRecord[String, String](topic, recordWithAlternativeDelimiterAsValueList(idx)))
      idx += 1
    }
  }

  it should "send correct messages to correct topics when sending each value individually - first data column != 0" in {
    val dataProducerThread = initializeDefaultDataProducerThread()
    when(mockedDataReader.getLine).thenReturn(Some(defaultRecord))
    dataProducerThread.run()
    for (idx <- defaultColumnStartOption.get to defaultColumnEndOption.get) {
      verify(mockedKafkaProducer, times(1)).send(new ProducerRecord[String, String](defaultTopicList(idx), recordAsValueArray(idx)))
    }
  }

  it should "log an error und do not send the message if there are more columns defined than values exist" in {
    val dataProducerThread = initializeDefaultDataProducerThread(columnStartOption = columnOptionNull, columnEndOption = columnOptionNull)
    val val1 = "FC"
    val val2 = "Magdeburg"
    when(mockedDataReader.getLine).thenReturn(Some(s"$val1 $val2"))
    val mockedLogger: Logger = mock[Logger]
    dataProducerThread.logger = mockedLogger
    dataProducerThread.run()
    verify(mockedLogger, times(1)).error(org.mockito.ArgumentMatchers.any[String])
    verify(mockedKafkaProducer, times(0)).send(org.mockito.ArgumentMatchers.any[ProducerRecord[String, String]])
  }

  it should "log an error und do not send the message if there are more columns defined than values exist; first data col != 0)" in {
    val dataProducerThread = initializeDefaultDataProducerThread(columnEndOption = columnOptionNull)
    val val0 = "1."
    val val1 = "FC"
    val val2 = "Magdeburg"
    val val3 = "FCM"
    when(mockedDataReader.getLine).thenReturn(Some(s"$val0 $val1 $val2 $val3"))
    val mockedLogger: Logger = mock[Logger]
    dataProducerThread.logger = mockedLogger
    dataProducerThread.run()
    verify(mockedLogger, times(1)).error(org.mockito.ArgumentMatchers.any[String])
    verify(mockedKafkaProducer, times(0)).send(org.mockito.ArgumentMatchers.any[ProducerRecord[String, String]])
  }

  it should "log an error und do not send the message if there are more values than columns specified" in {
    val tooSmallTopicList: List[String] = List("1.", "FC", "Magdeburg")
    val dataProducerThread = initializeDefaultDataProducerThread(columnList = tooSmallTopicList, columnStartOption = columnOptionNull, columnEndOption =
      None: Option[Int])
    when(mockedDataReader.getLine).thenReturn(Some(defaultRecord))
    val mockedLogger: Logger = mock[Logger]
    dataProducerThread.logger = mockedLogger
    dataProducerThread.run()
    verify(mockedLogger, times(1)).error(org.mockito.ArgumentMatchers.any[String])
    verify(mockedKafkaProducer, times(0)).send(org.mockito.ArgumentMatchers.any[ProducerRecord[String, String]])
  }

  it should "log an error und do not send the message if there are more values than columns specified; first data col != 0)" in {
    val tooSmallTopicList: List[String] = List("1.", "FC", "Magdeburg")
    val dataProducerThread = initializeDefaultDataProducerThread(columnList = tooSmallTopicList, columnEndOption = columnOptionNull)
    when(mockedDataReader.getLine).thenReturn(Some(defaultRecord))
    val mockedLogger: Logger = mock[Logger]
    dataProducerThread.logger = mockedLogger
    dataProducerThread.run()
    verify(mockedLogger, times(1)).error(org.mockito.ArgumentMatchers.any[String])
    verify(mockedKafkaProducer, times(0)).send(org.mockito.ArgumentMatchers.any[ProducerRecord[String, String]])
  }

  it should "mark line as invalid if column idx exceeds range of available data columns (in case single col shall be sent)" in {
    val dataProducerThread = initializeDefaultDataProducerThread()
    assert(!dataProducerThread.isLineValid("1. FCM"))
  }

  it should "mark line as invalid and print error if each value send individually - more values than topics" in {
    val dataProducerThread = initializeDefaultDataProducerThread(columnList = List[String]("FC", "Magdeburg"), columnStartOption =
      columnOptionNull, columnEndOption = columnOptionNull)
    val mockedLogger: Logger = mock[Logger]
    dataProducerThread.logger = mockedLogger
    assert(!dataProducerThread.isLineValid(defaultRecord))
    verify(mockedLogger, times(1)).error(org.mockito.ArgumentMatchers.any[String])
  }

  it should "mark line as invalid and print error if each value send individually - more topics than values" in {
    val dataProducerThread = initializeDefaultDataProducerThread(columnStartOption = columnOptionNull, columnEndOption = columnOptionNull)
    val mockedLogger: Logger = mock[Logger]
    dataProducerThread.logger = mockedLogger
    assert(!dataProducerThread.isLineValid("1. FCM"))
    verify(mockedLogger, times(1)).error(org.mockito.ArgumentMatchers.any[String])
  }

  it should "mark line as valid if each value send individually - n.o. topics == n.o. values" in {
    val dataProducerThread = initializeDefaultDataProducerThread()
    val mockedLogger: Logger = mock[Logger]
    dataProducerThread.logger = mockedLogger
    assert(dataProducerThread.isLineValid(defaultRecord))
    verify(mockedLogger, times(0)).warn(org.mockito.ArgumentMatchers.any[String])
  }
}
