package org.hpi.esb.datavalidator.consumer

import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class Records(topics: List[String]) {

  val recordsByTopic: mutable.Map[String, ListBuffer[ConsumerRecord[String, String]]] = createRecordsByTopicMap(topics)

  private def createRecordsByTopicMap(topics: List[String]) = {
    val immutableMap = topics.map(t => (t, new ListBuffer[ConsumerRecord[String, String]]())).toMap
    mutable.Map(immutableMap.toSeq: _*)
  }

  def getTopicResults(topic: String): ListBuffer[ConsumerRecord[String, String]] = {
    recordsByTopic.getOrElse(topic, new ListBuffer[ConsumerRecord[String, String]]())
  }
}
