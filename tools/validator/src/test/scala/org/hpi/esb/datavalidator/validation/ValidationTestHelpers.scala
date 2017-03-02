package org.hpi.esb.datavalidator.validation

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.record.TimestampType

import scala.collection.mutable.ListBuffer

trait ValidationTestHelpers {

  // create a list of ConsumerRecord objects
  def createConsumerRecordList(topic: String, values: List[(Long, String)]): ListBuffer[ConsumerRecord[String, String]] = {
    val l = new ListBuffer[ConsumerRecord[String, String]]()
    values.foreach { case (timestamp, value) => l.append(createConsumerRecord(topic, timestamp, value)) }
    l
  }

  def createConsumerRecord(topic: String, timestamp: Long, value: String): ConsumerRecord[String, String] = {
    val partition = 0
    val offset = 0
    val checksum = 0
    val serializedKeySize = 0
    val serializedValueSize = 0
    val key = "0"

    new ConsumerRecord[String, String](topic, partition, offset, timestamp, TimestampType.CREATE_TIME, checksum, serializedKeySize, serializedValueSize, key, value)
  }
}
