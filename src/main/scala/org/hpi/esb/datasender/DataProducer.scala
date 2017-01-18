package org.hpi.esb.datasender

import java.util.Properties
import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.hpi.esb.util.Logging


class DataProducer(producerConfig: DataProducerConfig) extends Logging {

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.30.208:9092,192.168.30.207:9092,192.168.30.141:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.ACKS_CONFIG, "0")
  props.put(ProducerConfig.BATCH_SIZE_CONFIG, "1")

  val producer = new KafkaProducer[String, String](props)
  val executor: ScheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1)
  val dataReader = new DataReader(producerConfig.dataInputPath)

  var t: ScheduledFuture[_] = _

  def shutDown() = {
    t.cancel(false)
    dataReader.close()
    producer.close()
    executor.shutdown()
    logger.info("Shut data producer down.")
  }

  def execute(): Unit = {
    val initialDelay = 0
    val unit = TimeUnit.MICROSECONDS
    val producerThread = new DataProducerThread(this, producer, dataReader, producerConfig.kafkaTopic, producerConfig.sendWholeMessage)

    t = executor.scheduleAtFixedRate(producerThread, initialDelay, producerConfig.dataProducerPeriod, unit)
    logger.info("Start sending messages to Apache Kafka.")
  }
}
