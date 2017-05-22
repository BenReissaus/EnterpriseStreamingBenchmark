package org.hpi.esb.datasender

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.hpi.esb.commons.config.Configs
import org.hpi.esb.commons.util.Logging
import org.hpi.esb.datasender.config.{Config, DataReaderConfig, DataSenderConfig, KafkaProducerConfig}
import org.hpi.esb.datasender.metrics.MetricHandler
import org.hpi.esb.util.OffsetManagement

import scala.io.Source

class DataDriver(config: Config) extends Logging {

  private val topics = Configs.benchmarkConfig.sourceTopics
  private val scaleFactor = Configs.benchmarkConfig.scaleFactor.toString
  private val dataReader = createDataReader(config.dataReaderConfig)
  private val kafkaProducerProperties = createKafkaProducerProperties(config.kafkaProducerConfig)
  private val kafkaProducer = new KafkaProducer[String, String](kafkaProducerProperties)
  private val dataProducer = createDataProducer(kafkaProducer, dataReader, config.dataSenderConfig)

  def run(): Unit = {
    dataProducer.execute()
  }

  def createKafkaProducerProperties(kafkaProducerConfig: KafkaProducerConfig): Properties = {

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProducerConfig.bootstrapServers.get)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kafkaProducerConfig.keySerializerClass.get)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kafkaProducerConfig.valueSerializerClass.get)
    props.put(ProducerConfig.ACKS_CONFIG, kafkaProducerConfig.acks.get)
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, kafkaProducerConfig.batchSize.get.toString)
    props.put(ProducerConfig.LINGER_MS_CONFIG, kafkaProducerConfig.lingerTime.toString)
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, kafkaProducerConfig.bufferMemorySize.toString)
    props
  }

  def createDataReader(dataReaderConfig: DataReaderConfig): DataReader = {
    new DataReader(Source.fromFile(dataReaderConfig.dataInputPath.get),
      dataReaderConfig.columns.get,
      dataReaderConfig.columnDelimiter.get,
      dataReaderConfig.dataColumnStart.get,
      dataReaderConfig.readInRam)
  }

  def createDataProducer(kafkaProducer: KafkaProducer[String, String], dataReader: DataReader,
                         dataSenderConfig: DataSenderConfig): DataProducer = {

    val numberOfThreads = config.dataSenderConfig.numberOfThreads.get
    val sendingInterval = config.dataSenderConfig.sendingInterval.get
    val singleColumnMode = config.dataSenderConfig.singleColumnMode

    new DataProducer(this, kafkaProducer, dataReader, topics,
      numberOfThreads, sendingInterval, singleColumnMode, config.dataSenderConfig.timeUnit)
  }

  def printMetrics(expectedRecordNumber: Int): Unit = {
    val ack = kafkaProducerProperties.get(ProducerConfig.ACKS_CONFIG).asInstanceOf[String]
    val batchSize = kafkaProducerProperties.get(ProducerConfig.BATCH_SIZE_CONFIG).asInstanceOf[String]
    val sendingInterval = config.dataSenderConfig.sendingInterval.get.toString
    val metricHandler: MetricHandler = new MetricHandler(kafkaProducer, topics, scaleFactor, ack,
      batchSize, sendingInterval, expectedRecordNumber)

    metricHandler.execute()
  }
}
