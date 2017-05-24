package org.hpi.esb.datasender

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.hpi.esb.commons.config.Configs
import org.hpi.esb.commons.util.Logging
import org.hpi.esb.datasender.config._

import scala.io.Source

class DataDriver(config: Config) extends Logging {

  private val topics = Configs.benchmarkConfig.sourceTopics
  private val dataReader = createDataReader(config.dataReaderConfig)
  private val kafkaProducerProperties = createKafkaProducerProperties(config.kafkaProducerConfig)
  private val kafkaProducer = new KafkaProducer[String, String](kafkaProducerProperties)
  private val resultHandler = new ResultHandler(config, Configs.benchmarkConfig, kafkaProducer)
  private val dataProducer = createDataProducer(kafkaProducer, dataReader, resultHandler)

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
                         resultHandler: ResultHandler): DataProducer = {

    val numberOfThreads = config.dataSenderConfig.numberOfThreads.get
    val sendingInterval = config.dataSenderConfig.sendingInterval.get
    val sendingIntervalTimeUnit = config.dataSenderConfig.getSendingIntervalTimeUnit()
    val duration = config.dataSenderConfig.duration
    val durationTimeUnit = config.dataSenderConfig.getDurationTimeUnit()
    val singleColumnMode = config.dataSenderConfig.singleColumnMode

    new DataProducer(resultHandler, kafkaProducer, dataReader, topics, numberOfThreads,
      sendingInterval, sendingIntervalTimeUnit, duration, durationTimeUnit, singleColumnMode)
  }
}
