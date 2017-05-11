package org.hpi.esb.flink.kafka

import java.util.concurrent.atomic.AtomicInteger

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.flink.api.scala._
import org.hpi.esb.flink.CustomAssigner

object KafkaConsumer {
  var id = new AtomicInteger(0)
}
class KafkaConsumer(env: StreamExecutionEnvironment, consumerTopic: String) extends KafkaConnector {

  val id = KafkaConsumer.id.incrementAndGet()
  // TODO: read properties from file
  props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
  props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, s"Flink_ESB - $id - ${System.currentTimeMillis}")
  props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val stream = env
    .addSource(new FlinkKafkaConsumer010(consumerTopic, new SimpleStringSchema(), props))
    .assignTimestampsAndWatermarks(new CustomAssigner())

  def consume() = {
    stream
  }
}
