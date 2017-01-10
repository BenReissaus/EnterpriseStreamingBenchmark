package org.hpi.esb.datasender

import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.hpi.esb.util.Logging

class DataProducerThread(dataProducer: DataProducer, producer: KafkaProducer[String, String], dataReader: DataReader, topic: String) extends Runnable with Logging {

  var t: ScheduledFuture[_] = _

  def run() {
    val line = dataReader.getLine()
    if (line != null) {
      val value = line.split("\\s+")(3)
      val message = new ProducerRecord[String, String](topic, value)

      producer.send(message)
      logger.debug(s"Sent value $value.")
    }
    else {
      logger.info(s"Found end of data file.")
      t.cancel(false)
      dataProducer.shutDown()
    }
  }

  def call(executor: ScheduledThreadPoolExecutor, initialDelay: Long, period: Long, unit: TimeUnit) = {
    logger.info("Start sending messages to Apache Kafka.")
    t = executor.scheduleAtFixedRate(this, initialDelay, period, unit)
  }
}
