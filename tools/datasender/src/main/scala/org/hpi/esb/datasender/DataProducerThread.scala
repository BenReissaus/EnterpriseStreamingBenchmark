package org.hpi.esb.datasender

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.hpi.esb.commons.output.ValueFormatter.roundPrecise
import org.hpi.esb.commons.util.Logging

class DataProducerThread(dataProducer: DataProducer, kafkaProducer: KafkaProducer[String, String],
                         dataReader: DataReader, topics: List[String]) extends Runnable with Logging {

  val scaleFactor: Int = Option(topics).getOrElse(List()).length
  var totalSuccessfulSends = 0
  var totalSends = 0

  def run() {
    if (dataReader.hasRecords) {
      send(scale(dataReader.getRecords))
    } else {
      logger.info(s"Send attempts: $totalSends Failed Total: $getFailedSends Failed Percentage: ${roundPrecise(getFailedSendsPercentage, precision = 3)}%")
      dataProducer.shutDown()
    }
  }

  def send(messagesOption: Option[List[String]]): Unit = {
    messagesOption.foreach(messages => {
      messages.zip(topics)
        .foreach {
          case (message, topic) =>
            sendToKafka(message, topic)
            totalSends += 1
        }
    })
  }

  def sendToKafka(value: String, topic: String): Unit = {
    val message = new ProducerRecord[String, String](topic, value)
    kafkaProducer.send(message, new Callback() {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        totalSuccessfulSends += 1
      }
    })
    logger.debug(s"Sent value $value to topic $topic.")
  }

  def scale(messages: Option[List[String]]): Option[List[String]] = {
    messages.map(_.take(scaleFactor))
  }

  def getFailedSends: Int = totalSends - totalSuccessfulSends

  def getFailedSendsPercentage: BigDecimal =
    if (totalSends != 0) {
      BigDecimal(getFailedSends) / BigDecimal(totalSends)
    } else {
      0
    }

}
