package org.hpi.esb.datasender

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.hpi.esb.util.Logging

class DataProducerThread(dataProducer: DataProducer, producer: KafkaProducer[String, String], dataReader: DataReader, topic: String, columnToBeSend: Int, columnDelimiter: String)
  extends Runnable with Logging {

  def run() {
    val line = dataReader.getLine
    if (line != null) {
      var value = line
      if (columnToBeSend >= 0)
        value = line.split(s"\\${columnDelimiter}")(columnToBeSend)

      val message = new ProducerRecord[String, String](topic, value)

      producer.send(message)
      logger.debug(s"Sent value $value.")
    } else {
      logger.info("Found end of data file.")
      dataProducer.shutDown
    }
  }
}
