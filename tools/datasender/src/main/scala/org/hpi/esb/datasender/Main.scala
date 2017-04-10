package org.hpi.esb.datasender

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.hpi.esb.config._
import org.hpi.esb.util.Logging
import org.hpi.esb.commons.config.Configs

import scala.io.Source


object Main extends Logging {


  def main(args: Array[String]) : Unit = {

    val config = ConfigHandler.getConfig(args)
    setLogLevel(config.verbose)

    val dataReader = getDataReader(config.dataReaderConfig)
    val kafkaProducer = getKafkaProducer(config.kafkaProducerConfig)

    val topics: List[String] = Configs.benchmarkConfig.getSourceTopics
    val seperator = " "
    logger.info(s"Sending records to following topics: ${topics.mkString(seperator)}")
    val numberOfThreads = config.dataSenderConfig.numberOfThreads.get
    val sendingInterval = config.dataSenderConfig.sendingInterval.get

    val dataProducer = new DataProducer(kafkaProducer, dataReader, topics,
      numberOfThreads, sendingInterval)

    dataProducer.execute()
  }

  def getDataReader(dataReaderConfig: DataReaderConfig): DataReader = {
    new DataReader(Source.fromFile(dataReaderConfig.dataInputPath.get),
      dataReaderConfig.columns.get,
      dataReaderConfig.columnDelimiter.get,
      dataReaderConfig.dataColumnStart.get)
  }

  def getKafkaProducer(kafkaProducerConfig: KafkaProducerConfig): KafkaProducer[String, String] = {

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProducerConfig.bootstrapServers.get)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kafkaProducerConfig.keySerializerClass.get)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kafkaProducerConfig.valueSerializerClass.get)
    props.put(ProducerConfig.ACKS_CONFIG, kafkaProducerConfig.acks.get)
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, kafkaProducerConfig.batchSize.get.toString)

    new KafkaProducer[String, String](props)
  }

  def setLogLevel(verbose: Boolean): Unit = {
    if (verbose) {
      Logging.setToDebug
      logger.info("DEBUG/VERBOSE mode switched on")
    }
  }

}
