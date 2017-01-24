package org.hpi.esb.datasender

import java.util.Properties
import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.hpi.esb.conf.configs.DataSenderConfig
import org.hpi.esb.util.Logging


class DataProducer(producerConfig: DataSenderConfig) extends Logging {

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, producerConfig.kafkaProducer.bootstrapServers)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, producerConfig.kafkaProducer.keySerializerClass)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, producerConfig.kafkaProducer.valueSerializerClass)
  props.put(ProducerConfig.ACKS_CONFIG, producerConfig.kafkaProducer.acks)
  props.put(ProducerConfig.BATCH_SIZE_CONFIG, producerConfig.kafkaProducer.batchSize)

  val producer = new KafkaProducer[String, String](props)
  val executor: ScheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(producerConfig.numberOfThreads) //passing number of threads in pool
  val dataReader = new DataReader(producerConfig.dataInputPath)

  var t: ScheduledFuture[_] = _

  def shutDown() = {
    t.cancel(false)
    dataReader.close
    producer.close()
    executor.shutdown()
    logger.info("Shut data producer down.")
  }

  def execute(): Unit = {
    val initialDelay = 0
    val producerThread = new DataProducerThread(this, producer, dataReader, producerConfig.kafkaTopic, producerConfig.columnToBeSend, producerConfig.columnDelimiter)

    t = executor.scheduleAtFixedRate(producerThread, initialDelay, producerConfig.sendingInterval, TimeUnit.MICROSECONDS)
    logger.info("Start sending messages to Apache Kafka.")
  }
}
