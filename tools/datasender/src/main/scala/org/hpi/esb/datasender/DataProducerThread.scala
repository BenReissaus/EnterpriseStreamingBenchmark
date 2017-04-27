package org.hpi.esb.datasender

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.hpi.esb.commons.output.ValueFormatter.roundPrecise
import org.hpi.esb.commons.util.Logging

class DataProducerThread(dataProducer: DataProducer, kafkaProducer: KafkaProducer[String, String],
                         dataReader: DataReader, topics: List[String], singleColumnMode: Boolean) extends Runnable with Logging {

  val scaleFactor: Int = Option(topics).getOrElse(List()).length
  var totalSuccessfulSends = 0
  var totalSends = 0

  def run() {
    if (dataReader.hasRecords) {
      send(dataReader.getRecords)
    } else {
      logger.info(s"Send attempts: $totalSends Failed Total: $getFailedSends Failed Percentage: ${roundPrecise(getFailedSendsPercentage, precision = 3)}%")
      dataProducer.shutDown()
    }
  }

  def send(messagesOption: Option[List[String]]): Unit = {
    messagesOption.foreach(messages => {
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
        sendToKafka(topic=topic, message=message)
        totalSends += 1
      })
  }

  def sendMultiColumns(messages: List[String]): Unit = {
    messages.zip(topics)
      .foreach {
        case (message, topic) =>
          sendToKafka(topic=topic, message=message)
          totalSends += 1
      }
  }

  def sendToKafka(topic: String, message: String): Unit = {
    val record = new ProducerRecord[String, String](topic, message)
    kafkaProducer.send(record, new Callback() {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        totalSuccessfulSends += 1
      }
    })
    logger.debug(s"Sent value $message to topic $topic.")
  }

  def getFailedSends: Int = totalSends - totalSuccessfulSends

  def getFailedSendsPercentage: BigDecimal =
    if (totalSends != 0) {
      BigDecimal(getFailedSends) / BigDecimal(totalSends)
    } else {
      0
    }

}
