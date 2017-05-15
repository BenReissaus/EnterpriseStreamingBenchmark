package org.hpi.esb.flink.kafka

import java.util.concurrent.atomic.AtomicInteger

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.flink.api.scala._
import org.hpi.esb.flink.CustomAssigner

class KafkaConsumer(env: StreamExecutionEnvironment, consumerTopic: String) extends KafkaConnector {

  // using a UUID is necessary to make sure that no other consumer has the same group ID
  val uuid = java.util.UUID.randomUUID.toString

  // TODO: read properties from file
  props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
  props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, s"$consumerTopic - $uuid")
  props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val stream = env
    .addSource(new FlinkKafkaConsumer010(consumerTopic, new SimpleStringSchema(), props))
    .assignTimestampsAndWatermarks(new CustomAssigner())

  def consume() = {
    stream
  }
}
