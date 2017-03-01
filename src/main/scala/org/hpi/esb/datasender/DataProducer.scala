package org.hpi.esb.datasender

import java.util.Properties
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.hpi.esb.util.Logging


class DataProducer(filePath: String) extends Logging {

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.30.208:9092,192.168.30.207:9092,192.168.30.141:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.ACKS_CONFIG, "0")
  props.put(ProducerConfig.BATCH_SIZE_CONFIG, "1")

  val producer = new KafkaProducer[String, String](props)
  val executor: ScheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1)
  val dataReader = new DataReader(filePath)

  val topic = "NEW_DEBS_IN"

  def shutDown() = {
    dataReader.close()
    producer.close()
    executor.shutdown()
    logger.info("Shut data producer down.")
  }

  def execute(period: Int): Unit = {

    val initialDelay = 0
    val unit = TimeUnit.NANOSECONDS

    val producerThread = new DataProducerThread(this, producer, dataReader, topic)
    producerThread.call(executor, initialDelay, period, unit)

    //  producer.metrics().foreach { case (m1, m2) => {
    //    println(s"metric name: ${m2.metricName()} value: ${m2.value()}")
    //  }}
  }
}
