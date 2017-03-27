package org.hpi.esb.datavalidator.data

import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.collection.mutable.ListBuffer
import scala.util.Try

trait Deserializer[T] {
  def deserialize(value: String, timestamp: Long): Try[T]
  def deserializeList(list: ListBuffer[ConsumerRecord[String, String]]): ListBuffer[Try[T]]
}
