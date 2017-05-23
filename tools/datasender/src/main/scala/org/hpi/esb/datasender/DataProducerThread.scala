package org.hpi.esb.datasender

import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.hpi.esb.commons.util.Logging

class DataProducerThread(dataProducer: DataProducer, kafkaProducer: KafkaProducer[String, String],
                         dataReader: DataReader, topics: List[String], singleColumnMode: Boolean,
                         duration: Long, durationTimeUnit: TimeUnit) extends Runnable with Logging {

  val scaleFactor: Int = Option(topics).getOrElse(List()).length
  var numberOfRecords: Int = 0
  val startTime: Long = currentTime
  val endTime: Long = startTime + durationTimeUnit.toMillis(duration)

  def currentTime: Long = System.currentTimeMillis()

  def run() {
    if (currentTime < endTime) {
      send(dataReader.readRecords)
    } else {
      logger.info(s"Shut down after $durationTimeUnit: $duration.")
      dataProducer.shutDown()
    }
  }

  def send(messagesOption: Option[List[String]]): Unit = {
    messagesOption.foreach(messages => {
      numberOfRecords += 1
      if (singleColumnMode) {
        sendSingleColumn(messages)
      } else {
        sendMultiColumns(messages)
      }
    })
  }

  def sendSingleColumn(messages: List[String]): Unit = {
    val message = messages.head
    topics.foreach(
      topic => {
        sendToKafka(topic = topic, message = message)
      })
  }

  def sendToKafka(topic: String, message: String): Unit = {
    val record = new ProducerRecord[String, String](topic, message)
    kafkaProducer.send(record)
    logger.debug(s"Sent value $message to topic $topic.")
  }

  def sendMultiColumns(messages: List[String]): Unit = {
    messages.zip(topics)
      .foreach {
        case (message, topic) =>
          sendToKafka(topic = topic, message = message)
      }
  }
}
