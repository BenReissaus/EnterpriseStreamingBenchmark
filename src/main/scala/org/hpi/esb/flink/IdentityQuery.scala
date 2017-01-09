package org.hpi.esb.flink

import java.util.Properties

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer010, FlinkKafkaConsumer010}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

object IdentityQuery extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val BOOTSTRAP_SERVERS = "192.168.30.208:9092,192.168.30.207:9092,192.168.30.141:9092"

  // consumer setup
  val consumerProps = new Properties()
  consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
  consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "Flink_ESB")
  val consumerTopic = "NEW_DEBS_IN"

  // producer setup
  val producerProps = new Properties()
  producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
  val producerTopic = "NEW_DEBS_OUT"

  // consume stream
  val stream = env.addSource(new FlinkKafkaConsumer010[String](consumerTopic, new SimpleStringSchema(), consumerProps))
  stream.print()

  // produce stream
  FlinkKafkaProducer010.writeToKafkaWithTimestamps(stream, producerTopic, new SimpleStringSchema(), producerProps)

  env.execute("Flink Kafka Example")
}
