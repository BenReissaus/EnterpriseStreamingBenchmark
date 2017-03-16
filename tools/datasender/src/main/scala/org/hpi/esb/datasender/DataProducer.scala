package org.hpi.esb.datasender

import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

import org.apache.kafka.clients.producer.KafkaProducer
import org.hpi.esb.util.Logging


class DataProducer(kafkaProducer: KafkaProducer[String, String], dataReader: DataReader,
                   topics: List[String], numberOfThreads: Int, sendingInterval: Int) extends Logging {


  val executor: ScheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(numberOfThreads)

  var t: ScheduledFuture[_] = _

  def shutDown(): Unit = {
    t.cancel(false)
    dataReader.close()
    kafkaProducer.close()
    executor.shutdown()
    logger.info("Shut data producer down.")
  }

  def execute(): Unit = {
    val initialDelay = 0
    val producerThread = new DataProducerThread(this, kafkaProducer, dataReader, topics)
    t = executor.scheduleAtFixedRate(producerThread, initialDelay, sendingInterval, TimeUnit.MICROSECONDS)
    logger.info("Start sending messages to Apache Kafka.")
  }
}
