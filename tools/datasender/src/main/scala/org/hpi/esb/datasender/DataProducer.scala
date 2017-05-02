package org.hpi.esb.datasender

import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

import org.apache.kafka.clients.producer.KafkaProducer
import org.hpi.esb.commons.util.Logging
import org.hpi.esb.datasender.config.Configurable


class DataProducer(dataDriver: DataDriver, kafkaProducer: KafkaProducer[String, String], dataReader: DataReader,
                   topics: List[String], numberOfThreads: Int, sendingInterval: Int, singleColumnMode: Boolean) extends Logging with Configurable {


  val executor: ScheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(numberOfThreads)
  val producerThread = new DataProducerThread(this, kafkaProducer, dataReader, topics, singleColumnMode)

  var t: ScheduledFuture[_] = _

  def shutDown(): Unit = {
    t.cancel(false)
    dataReader.close()
    kafkaProducer.close()
    executor.shutdown()
    logger.info("Shut data producer down.")
    dataDriver.printMetrics(expectedRecordNumber = producerThread.numberOfRecords)
  }


  def execute(): Unit = {
    val initialDelay = 0
    t = executor.scheduleAtFixedRate(producerThread, initialDelay, sendingInterval, TimeUnit.MICROSECONDS)
    val seperator = " "
    logger.info(s"Sending records to following topics: ${topics.mkString(seperator)}")
  }
}
