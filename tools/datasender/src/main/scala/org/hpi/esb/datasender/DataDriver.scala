package org.hpi.esb.datasender

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.hpi.esb.commons.config.Configs
import org.hpi.esb.commons.util.Logging
import org.hpi.esb.config.{Config, DataReaderConfig, DataSenderConfig, KafkaProducerConfig}
import org.hpi.esb.output.ResultWriter

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
    props
  }

  def createDataReader(dataReaderConfig: DataReaderConfig): DataReader = {
    new DataReader(Source.fromFile(dataReaderConfig.dataInputPath.get),
      dataReaderConfig.columns.get,
      dataReaderConfig.columnDelimiter.get,
      dataReaderConfig.dataColumnStart.get)
  }

  def createDataProducer(kafkaProducer: KafkaProducer[String, String], dataReader: DataReader,
                         dataSenderConfig: DataSenderConfig): DataProducer = {

    val numberOfThreads = config.dataSenderConfig.numberOfThreads.get
    val sendingInterval = config.dataSenderConfig.sendingInterval.get
    val singleColumnMode = config.dataSenderConfig.singleColumnMode

    new DataProducer(this, kafkaProducer, dataReader, topics, numberOfThreads, sendingInterval, singleColumnMode)
  }

  def printResults(): Unit = {
    val ack = kafkaProducerProperties.get(ProducerConfig.ACKS_CONFIG).asInstanceOf[String]
    val batchSize = kafkaProducerProperties.get(ProducerConfig.BATCH_SIZE_CONFIG).asInstanceOf[String]
    val sendingInterval = config.dataSenderConfig.sendingInterval.get.toString
    val resultPrinter: ResultWriter = new ResultWriter(kafkaProducer, scaleFactor, ack,
      batchSize, sendingInterval)

    resultPrinter.write()
  }
}
