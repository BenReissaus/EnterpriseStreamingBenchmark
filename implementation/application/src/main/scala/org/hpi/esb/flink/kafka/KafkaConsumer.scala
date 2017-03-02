package org.hpi.esb.flink.kafka

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.flink.api.scala._
import org.hpi.esb.flink.CustomAssigner

class KafkaConsumer(env: StreamExecutionEnvironment, consumerTopic: String) extends KafkaConnector {

  // TODO: read properties from file
  props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
  props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "Flink_ESB")

  val stream = env
    .addSource(new FlinkKafkaConsumer010(consumerTopic, new SimpleStringSchema(), props))
    .assignTimestampsAndWatermarks(new CustomAssigner())

  def consume() = {
    stream
  }
}
