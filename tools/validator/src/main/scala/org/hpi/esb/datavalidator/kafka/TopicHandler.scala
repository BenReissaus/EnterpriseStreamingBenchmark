package org.hpi.esb.datavalidator.kafka

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.hpi.esb.util.OffsetManagement

case class TopicHandler(topicName: String, numberOfMessages: Long, topicSource: Source[ConsumerRecord[String, String], Consumer.Control])

object TopicHandler {

  def create(topicName: String, system: ActorSystem): TopicHandler = {

    val consumerSettings: ConsumerSettings[String, String] = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
      .withBootstrapServers("192.168.30.208:9092,192.168.30.207:9092,192.168.30.141:9092")
      .withGroupId("group1")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val partition = 0
    val topicSource = createSource(consumerSettings, topicName, partition)
    val numberOfMessages = OffsetManagement.getNumberOfMessages(topicName, partition)

    new TopicHandler(topicName, numberOfMessages, topicSource)
  }

  def createSource(consumerSettings: ConsumerSettings[String, String], topicName: String, partition: Int) = {

    val subscription = Subscriptions.assignmentWithOffset(
      new TopicPartition(topicName, partition) -> 0L
    )
    Consumer.plainSource(consumerSettings, subscription)
  }
}
