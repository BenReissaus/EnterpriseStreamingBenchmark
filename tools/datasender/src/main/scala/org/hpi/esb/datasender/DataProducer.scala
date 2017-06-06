package org.hpi.esb.datasender

import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

import org.apache.kafka.clients.producer.KafkaProducer
import org.hpi.esb.commons.util.Logging
import org.hpi.esb.datasender.config.Configurable
import org.hpi.esb.datasender.output.writers.DatasenderRunResultWriter
import org.hpi.esb.util.OffsetManagement


class DataProducer(resultHandler: DatasenderRunResultWriter, kafkaProducer: KafkaProducer[String, String],
                   dataReader: DataReader, topics: List[String], numberOfThreads: Int,
                   sendingInterval: Int, sendingIntervalTimeUnit: TimeUnit,
                   duration: Long, durationTimeUnit: TimeUnit, singleColumnMode: Boolean) extends Logging with Configurable {


  val executor: ScheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(numberOfThreads)
  val producerThread = new DataProducerThread(this, kafkaProducer, dataReader, topics,
    singleColumnMode, duration, durationTimeUnit)

  val topicOffsets = getTopicOffsets()

  var t: ScheduledFuture[_] = _

  def shutDown(): Unit = {
    t.cancel(false)
    dataReader.close()
    kafkaProducer.close()
    executor.shutdown()
    logger.info("Shut data producer down.")
    val expectedRecordNumber = producerThread.numberOfRecords
    resultHandler.outputResults(topicOffsets, expectedRecordNumber)
  }

  def execute(): Unit = {
    val initialDelay = 0
    t = executor.scheduleAtFixedRate(producerThread, initialDelay, sendingInterval, sendingIntervalTimeUnit)
    val allTopics = topics.mkString(" ")
    logger.info(s"Sending records to following topics: $allTopics")
  }

  def getTopicOffsets(): Map[String, Long] = {
    topics.map(topic => {
      val currentOffset = OffsetManagement.getNumberOfMessages(topic, partition = 0)
      topic -> currentOffset
    }).toMap[String, Long]
  }
}
