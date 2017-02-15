package org.hpi.esb.conf

import org.apache.log4j.Logger
import org.hpi.esb.conf.Config.{DataModelConfig, DataSenderConfig, KafkaProducerConfig}
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers, PrivateMethodTester}
import org.scalatest.mockito.MockitoSugar

class ConfigTest extends FlatSpec with Matchers with PrivateMethodTester with BeforeAndAfter with MockitoSugar {

  val topicList: List[String] = List("1", ".", "F", "C", "Magdeburg", "FCM")
  val dataModelConfig: DataModelConfig = DataModelConfig(topicList, None: Option[Int], None: Option[Int])
  val dataModelConfigNoColumns: DataModelConfig = DataModelConfig(List[String](), None: Option[Int], None: Option[Int])
  val dataModelConfigTooBigFirstDataColumn: DataModelConfig = DataModelConfig(topicList, Some(6), None: Option[Int])
  val dataModelConfigTooSmallFirstDataColumn: DataModelConfig = DataModelConfig(topicList, Some(-1), None: Option[Int])
  val kafkaProducerConfigAcksZeroBatchSizeZero: KafkaProducerConfig = KafkaProducerConfig("server", "keySerializer", "valueSerializer", "0", 0)
  val kafkaProducerConfigAcksMinusOne: KafkaProducerConfig = KafkaProducerConfig("server", "keySerializer", "valueSerializer", "-1", 0)
  val kafkaProducerConfigAcksOne: KafkaProducerConfig = KafkaProducerConfig("server", "keySerializer", "valueSerializer", "1", 0)
  val kafkaProducerConfigAcksAll: KafkaProducerConfig = KafkaProducerConfig("server", "keySerializer", "valueSerializer", "all", 0)
  val kafkaProducerConfigEmptyBootstrapServers: KafkaProducerConfig = KafkaProducerConfig(" ", "keySerializer", "valueSerializer", "0", 0)
  val kafkaProducerConfigEmptyKeySerializer: KafkaProducerConfig = KafkaProducerConfig("server", " ", "valueSerializer", "0", 0)
  val kafkaProducerConfigEmptyValueSerializer: KafkaProducerConfig = KafkaProducerConfig("server", "keySerializer", " ", "0", 0)
  val kafkaProducerConfigAcksValueInvalid: KafkaProducerConfig = KafkaProducerConfig("server", "keySerializer", "valueSerializer", "1965", 0)
  val kafkaProducerConfigBatchSizeNegative: KafkaProducerConfig = KafkaProducerConfig("server", "keySerializer", "valueSerializer", "0", -1)
  val dataSenderConfigSendingIntervalInvalid: DataSenderConfig = DataSenderConfig(dataInputPath = "", sendingInterval = -1, columnDelimiter = ";",numberOfThreads = 1, kafkaProducer = null, dataModel = null)
  val dataSenderConfigNumberOfThreadsInvalid: DataSenderConfig = DataSenderConfig(dataInputPath = "", sendingInterval = 0, columnDelimiter = ";", numberOfThreads = -1, kafkaProducer = null, dataModel = null)
  val dataSenderConfigValid: DataSenderConfig = DataSenderConfig(dataInputPath = "", sendingInterval = 0, columnDelimiter = ";", numberOfThreads = 1, kafkaProducer = null, dataModel = null)

  "Config.checkGreaterOrEqual" should "return true if value greater than limit" in {
    val mockedLogger: Logger = mock[Logger]
    Config.logger = mockedLogger
    assert(Config.checkGreaterOrEqual("testAttribute", attributeValue = 2, valueMinimum = 1))
    verify(mockedLogger, times(0)).error(org.mockito.ArgumentMatchers.any[String])
  }

  it should "return true if value equals limit" in {
    val mockedLogger: Logger = mock[Logger]
    Config.logger = mockedLogger
    assert(Config.checkGreaterOrEqual("testAttribute", attributeValue = -1, valueMinimum = -1))
    verify(mockedLogger, times(0)).error(org.mockito.ArgumentMatchers.any[String])
  }

  it should "return false if value is below limit" in {
    val mockedLogger: Logger = mock[Logger]
    Config.logger = mockedLogger
    assert(!Config.checkGreaterOrEqual("testAttribute", attributeValue = -1, valueMinimum = 0))
    verify(mockedLogger, times(1)).error(org.mockito.ArgumentMatchers.any[String])
  }

  "Config.checkAttributeHasValue" should "return true if attribute contains value" in {
    val mockedLogger: Logger = mock[Logger]
    Config.logger = mockedLogger
    assert(Config.checkAttributeHasValue("testAttribute", attributeValue = "1. FC Magdeburg"))
    verify(mockedLogger, times(0)).error(org.mockito.ArgumentMatchers.any[String])
  }

  it should "return false if attribute is empty" in {
    val mockedLogger: Logger = mock[Logger]
    Config.logger = mockedLogger
    assert(!Config.checkAttributeHasValue("testAttribute", attributeValue = " "))
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
    Config.logger = mockedLogger
    assert(!kafkaProducerConfigEmptyBootstrapServers.areServerAndSerializerAttributesValid)
    verify(mockedLogger, times(1)).error(org.mockito.ArgumentMatchers.any[String])
  }

  it should "return false if attribute keySerializer is empty" in {
    val mockedLogger: Logger = mock[Logger]
    Config.logger = mockedLogger
    assert(!kafkaProducerConfigEmptyKeySerializer.areServerAndSerializerAttributesValid)
    verify(mockedLogger, times(1)).error(org.mockito.ArgumentMatchers.any[String])
  }

  it should "return false if attribute valueSerializer is empty" in {
    val mockedLogger: Logger = mock[Logger]
    Config.logger = mockedLogger
    assert(!kafkaProducerConfigEmptyValueSerializer.areServerAndSerializerAttributesValid)
    verify(mockedLogger, times(1)).error(org.mockito.ArgumentMatchers.any[String])
  }

  it should "return true if everything is fine" in {
    val mockedLogger: Logger = mock[Logger]
    Config.logger = mockedLogger
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
    Config.logger = mockedLogger
    assert(kafkaProducerConfigAcksZeroBatchSizeZero.isBatchSizeValid)
    verify(mockedLogger, times(0)).error(org.mockito.ArgumentMatchers.any[String])
  }

  it should "return false if batch size < 0" in {
    val mockedLogger: Logger = mock[Logger]
    Config.logger = mockedLogger
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
