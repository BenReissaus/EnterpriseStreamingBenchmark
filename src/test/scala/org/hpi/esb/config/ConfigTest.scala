package org.hpi.esb.config

import org.apache.log4j.Logger
import org.hpi.esb.config
import org.mockito.Mockito.{times, verify}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers, PrivateMethodTester}

class ConfigTest extends FlatSpec with Matchers with PrivateMethodTester with BeforeAndAfter with MockitoSugar {

  val topicList: List[String] = List("1", ".", "F", "C", "Magdeburg", "FCM")
  val dataModelConfig: DataModelConfig = DataModelConfig(Option(topicList), None: Option[Int], None: Option[Int])
  val dataModelConfigNoColumns: DataModelConfig = DataModelConfig(Option(List[String]()), None: Option[Int], None: Option[Int])
  val dataModelConfigTooBigFirstDataColumn: DataModelConfig = DataModelConfig(Option(topicList), Some(topicList.size), None: Option[Int])
  val dataModelConfigTooSmallFirstDataColumn: DataModelConfig = DataModelConfig(Option(topicList), Some(-1), None: Option[Int])
  val kafkaProducerConfigAcksZeroBatchSizeZero: KafkaProducerConfig = KafkaProducerConfig(Option("server"), Option("keySerializer"), Option("valueSerializer"), Option("0"), Option(0))
  val kafkaProducerConfigAcksMinusOne: KafkaProducerConfig = KafkaProducerConfig(Option("server"), Option("keySerializer"), Option("valueSerializer"), Option("-1"), Option(0))
  val kafkaProducerConfigAcksOne: KafkaProducerConfig = KafkaProducerConfig(Option("server"), Option("keySerializer"), Option("valueSerializer"), Option("1"), Option(0))
  val kafkaProducerConfigAcksAll: KafkaProducerConfig = KafkaProducerConfig(Option("server"), Option("keySerializer"), Option("valueSerializer"), Option("all"), Option(0))
  val kafkaProducerConfigEmptyBootstrapServers: KafkaProducerConfig = KafkaProducerConfig(Option(" "), Option("keySerializer"), Option("valueSerializer"), Option("0"), Option(0))
  val kafkaProducerConfigEmptyKeySerializer: KafkaProducerConfig = KafkaProducerConfig(Option("server"), Option(" "), Option("valueSerializer"), Option("0"), Option(0))
  val kafkaProducerConfigEmptyValueSerializer: KafkaProducerConfig = KafkaProducerConfig(Option("server"), Option("keySerializer"), Option(" "), Option("0"), Option(0))
  val kafkaProducerConfigAcksValueInvalid: KafkaProducerConfig = KafkaProducerConfig(Option("server"), Option("keySerializer"), Option("valueSerializer"), Option("1965"), Option(0))
  val kafkaProducerConfigBatchSizeNegative: KafkaProducerConfig = KafkaProducerConfig(Option("server"), Option("keySerializer"), Option("valueSerializer"), Option("0"), Option(-1))
  val dataSenderConfigSendingIntervalInvalid: DataSenderConfig = DataSenderConfig(dataInputPath = Option(""), sendingInterval = Option(-1), columnDelimiter = Option(";"),
    numberOfThreads = Option(1), kafkaProducer = mock[KafkaProducerConfig], dataModel = mock[DataModelConfig])
  val dataSenderConfigNumberOfThreadsInvalid: DataSenderConfig = DataSenderConfig(dataInputPath = Option(""), sendingInterval = Option(0), columnDelimiter = Option(";"),
    numberOfThreads = Option(-1), kafkaProducer = mock[KafkaProducerConfig], dataModel = mock[DataModelConfig])
  val dataSenderConfigValid: DataSenderConfig = DataSenderConfig(dataInputPath = Option(""), sendingInterval = Option(0), columnDelimiter = Option(";"),
    numberOfThreads = Option(1), kafkaProducer = mock[KafkaProducerConfig], dataModel = mock[DataModelConfig])

  "Config.checkGreaterOrEqual" should "return true if value greater than limit" in {
    val mockedLogger: Logger = mock[Logger]
    config.logger = mockedLogger
    assert(config.checkGreaterOrEqual("testAttribute", attributeValue = 2, valueMinimum = 1))
    verify(mockedLogger, times(0)).error(org.mockito.ArgumentMatchers.any[String])
  }

  it should "return true if value equals limit" in {
    val mockedLogger: Logger = mock[Logger]
    config.logger = mockedLogger
    assert(config.checkGreaterOrEqual("testAttribute", attributeValue = -1, valueMinimum = -1))
    verify(mockedLogger, times(0)).error(org.mockito.ArgumentMatchers.any[String])
  }

  it should "return false if value is below limit" in {
    val mockedLogger: Logger = mock[Logger]
    config.logger = mockedLogger
    assert(!config.checkGreaterOrEqual("testAttribute", attributeValue = -1, valueMinimum = 0))
    verify(mockedLogger, times(1)).error(org.mockito.ArgumentMatchers.any[String])
  }

  "Config.checkAttributeOptionHasValue" should "return true if attribute contains value" in {
    val mockedLogger: Logger = mock[Logger]
    config.logger = mockedLogger
    assert(config.checkAttributeOptionHasValue("testAttribute", attributeValue = Option("1. FC Magdeburg")))
    verify(mockedLogger, times(0)).error(org.mockito.ArgumentMatchers.any[String])
  }

  it should "return false if attribute is empty" in {
    val mockedLogger: Logger = mock[Logger]
    config.logger = mockedLogger
    assert(!config.checkAttributeOptionHasValue("testAttribute", attributeValue = Option(" ")))
    verify(mockedLogger, times(1)).error(org.mockito.ArgumentMatchers.any[String])
  }

  "DataModelConfig.isValid" should "return false if column list is empty" in {
    val mockedLogger: Logger = mock[Logger]
    dataModelConfigNoColumns.logger = mockedLogger
    assert(!dataModelConfigNoColumns.isValid)
    verify(mockedLogger, times(1)).error(org.mockito.ArgumentMatchers.any[String])
  }

  it should "return false if configured first data column exceeds last column idx" in {
    val mockedLogger: Logger = mock[Logger]
    dataModelConfigTooBigFirstDataColumn.logger = mockedLogger
    assert(!dataModelConfigTooBigFirstDataColumn.isValid)
    verify(mockedLogger, times(1)).error(org.mockito.ArgumentMatchers.any[String])
  }

  it should "return false if first data column is negative" in {
    val mockedLogger: Logger = mock[Logger]
    dataModelConfigTooSmallFirstDataColumn.logger = mockedLogger
    assert(!dataModelConfigTooSmallFirstDataColumn.isValid)
    verify(mockedLogger, times(1)).error(org.mockito.ArgumentMatchers.any[String])
  }

  it should "return true if everything is fine" in {
    val mockedLogger: Logger = mock[Logger]
    dataModelConfig.logger = mockedLogger
    assert(dataModelConfig.isValid)
    verify(mockedLogger, times(0)).error(org.mockito.ArgumentMatchers.any[String])
  }

  "KafkaProducerConfig.areServerAndSerializerAttributesValid" should "return false if attribute bootstrapServers is empty" in {
    val mockedLogger: Logger = mock[Logger]
    config.logger = mockedLogger
    assert(!kafkaProducerConfigEmptyBootstrapServers.areServerAndSerializerAttributesValid)
    verify(mockedLogger, times(1)).error(org.mockito.ArgumentMatchers.any[String])
  }

  it should "return false if attribute keySerializer is empty" in {
    val mockedLogger: Logger = mock[Logger]
    config.logger = mockedLogger
    assert(!kafkaProducerConfigEmptyKeySerializer.areServerAndSerializerAttributesValid)
    verify(mockedLogger, times(1)).error(org.mockito.ArgumentMatchers.any[String])
  }

  it should "return false if attribute valueSerializer is empty" in {
    val mockedLogger: Logger = mock[Logger]
    config.logger = mockedLogger
    assert(!kafkaProducerConfigEmptyValueSerializer.areServerAndSerializerAttributesValid)
    verify(mockedLogger, times(1)).error(org.mockito.ArgumentMatchers.any[String])
  }

  it should "return true if everything is fine" in {
    val mockedLogger: Logger = mock[Logger]
    config.logger = mockedLogger
    assert(kafkaProducerConfigAcksZeroBatchSizeZero.areServerAndSerializerAttributesValid)
    verify(mockedLogger, times(0)).error(org.mockito.ArgumentMatchers.any[String])
  }

  "KafkaProducerConfig.isAcksValid" should "return false if attribute acks contains invalid value" in {
    val mockedLogger: Logger = mock[Logger]
    kafkaProducerConfigAcksValueInvalid.logger = mockedLogger
    assert(!kafkaProducerConfigAcksValueInvalid.isAcksValid)
    verify(mockedLogger, times(1)).error(org.mockito.ArgumentMatchers.any[String])
  }

  it should "return true if acks == 1" in {
    val mockedLogger: Logger = mock[Logger]
    kafkaProducerConfigAcksZeroBatchSizeZero.logger = mockedLogger
    assert(kafkaProducerConfigAcksZeroBatchSizeZero.isAcksValid)
    verify(mockedLogger, times(0)).error(org.mockito.ArgumentMatchers.any[String])
  }

  it should "return true if acks == 0" in {
    val mockedLogger: Logger = mock[Logger]
    kafkaProducerConfigAcksZeroBatchSizeZero.logger = mockedLogger
    assert(kafkaProducerConfigAcksZeroBatchSizeZero.isAcksValid)
    verify(mockedLogger, times(0)).error(org.mockito.ArgumentMatchers.any[String])
  }

  it should "return true if acks == -1" in {
    val mockedLogger: Logger = mock[Logger]
    kafkaProducerConfigAcksMinusOne.logger = mockedLogger
    assert(kafkaProducerConfigAcksMinusOne.isAcksValid)
    verify(mockedLogger, times(0)).error(org.mockito.ArgumentMatchers.any[String])
  }

  it should "return true if acks == 'all'" in {
    val mockedLogger: Logger = mock[Logger]
    kafkaProducerConfigAcksAll.logger = mockedLogger
    assert(kafkaProducerConfigAcksAll.isAcksValid)
    verify(mockedLogger, times(0)).error(org.mockito.ArgumentMatchers.any[String])
  }

  "KafkaProducerConfig.isBatchSizeValid" should "return true if batch size >= 0" in {
    val mockedLogger: Logger = mock[Logger]
    config.logger = mockedLogger
    assert(kafkaProducerConfigAcksZeroBatchSizeZero.isBatchSizeValid)
    verify(mockedLogger, times(0)).error(org.mockito.ArgumentMatchers.any[String])
  }

  it should "return false if batch size < 0" in {
    val mockedLogger: Logger = mock[Logger]
    config.logger = mockedLogger
    assert(!kafkaProducerConfigBatchSizeNegative.isBatchSizeValid)
    verify(mockedLogger, times(1)).error(org.mockito.ArgumentMatchers.any[String])
  }

  "KafkaProducerConfig.isValid" should "return true if everything is valid" in {
    assert(kafkaProducerConfigAcksZeroBatchSizeZero.isValid)
  }

  it should "return false if server or serializer attributes are invalid" in {
    assert(!kafkaProducerConfigEmptyBootstrapServers.isValid)
  }

  it should "return false if acks are invalid" in {
    assert(!kafkaProducerConfigAcksValueInvalid.isValid)
  }

  it should "return false if batch size is invalid" in {
    assert(!kafkaProducerConfigBatchSizeNegative.isValid)
  }

  "DataSenderConfig.isSendingIntervalValid" should "return true if interval >= 0" in {
    assert(dataSenderConfigValid.isSendingIntervalValid)
  }

  it should "return false if interval <= 0" in {
    assert(!dataSenderConfigSendingIntervalInvalid.isSendingIntervalValid)
  }

  "DataSenderConfig.isNumberOfThreadsValid" should "return true if n.o. threads >= 1" in {
    assert(dataSenderConfigValid.isNumberOfThreadsValid)
  }

  it should "return false if interval <= 0" in {
    assert(!dataSenderConfigNumberOfThreadsInvalid.isNumberOfThreadsValid)
  }
}
