package org.hpi.esb.datavalidator.data

import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.collection.mutable.ListBuffer
import scala.util.Try

object SimpleRecord extends Deserializer[SimpleRecord] {
  override def deserialize(value: String, timestamp: Long): Try[SimpleRecord] = {
    Try(SimpleRecord(value.toLong, timestamp))
  }

  override def deserializeList(list: ListBuffer[ConsumerRecord[String, String]]): ListBuffer[Try[SimpleRecord]] = {
    list.map(r => deserialize(r.value(), r.timestamp()))
  }
}

case class SimpleRecord(value: Long, override val timestamp: Long) extends Record(timestamp)