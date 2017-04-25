package org.hpi.esb.datasender

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.hpi.esb.util.Logging

class DataProducerThread(dataProducer: DataProducer, kafkaProducer: KafkaProducer[String, String],
                         dataReader: DataReader,  topics: List[String]) extends Runnable with Logging {

  val scaleFactor: Int = Option(topics).getOrElse(List()).length

  def run() {
    if (dataReader.hasRecords) {
      send(scale(dataReader.getRecords))
    } else {
      logger.info("Sent all available records.")
      dataProducer.shutDown()
    }
  }

  def send(messagesOption: Option[List[String]]): Unit = {
    messagesOption.foreach(messages => {
      messages.zip(topics)
      .foreach {
        case (message, topic) => sendToKafka(message, topic)
      }
    })
  }

  def sendToKafka(value: String, topic: String): Unit = {
    val message = new ProducerRecord[String, String](topic, value)
    kafkaProducer.send(message)
    logger.debug(s"Sent value $value.")
  }

  def scale(messages: Option[List[String]]): Option[List[String]] = {
    messages.map(_.take(scaleFactor))
  }
}
