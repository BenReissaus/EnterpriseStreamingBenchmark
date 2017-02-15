package org.hpi.esb.datasender

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.hpi.esb.util.Logging

class DataProducerThread(dataProducer: DataProducer, kafkaProducer: KafkaProducer[String, String], dataReader: DataReader, topicList: List[String], columnDelimiter: String,
                         columnStartOption: Option[Int], columnEndOption: Option[Int]) extends Runnable with Logging {

  val sendWholeLine: Boolean = topicList.size == 1
  val colsRestricted: Boolean = columnStartOption.isDefined && columnEndOption.isDefined
  val columnStart: Int = columnStartOption.getOrElse(0)
  val columnEnd: Int = columnEndOption.getOrElse(topicList.size - 1)

  var msgArray: Array[String] = _

  def isLineValid(line: String): Boolean = {
    if (!sendWholeLine) {
      msgArray = line.split(s"\\$columnDelimiter")
      if (topicList.size > msgArray.length) {
        logger.error(s"There are less values available (${msgArray.length}) than columns defined (${topicList.size}) - ignoring record")
        return false
      } else if (topicList.size < msgArray.length) {
        logger.error(s"There are less topics defined (${topicList.size}) than values available (${msgArray.length}) - ignoring record")
        return false
      }
    } else {
       msgArray = Array(line)
    }
    true
  }

  def run() {
    val line = dataReader.getLine
    if (line != null) {
      checkAndSend(line)
    } else {
      logger.info("Found end of data file.")
      dataProducer.shutDown()
    }
  }

  def checkAndSend(line: String): Unit = {
    if (isLineValid(line)) {
      for (idx <- columnStart until Integer.min(msgArray.length, Integer.min(topicList.size, columnEnd+1))) {
        send(value = msgArray(idx), topic = topicList(idx))
      }
    }
  }

  def send(value: String, topic: String): Unit = {
    val message = new ProducerRecord[String, String](topic, value)
    kafkaProducer.send(message)
    logger.debug(s"Sent value $value.")
  }

}
