package org.hpi.esb.flink

import java.util.Properties

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer010, FlinkKafkaConsumer010}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

object IdentityQuery {

  def execute(consumerTopic: String, producerTopic: String): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val BOOTSTRAP_SERVERS = "192.168.30.208:9092,192.168.30.207:9092,192.168.30.141:9092"

    // consumer setup
    val consumerProps = new Properties()
    consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
    consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "Flink_ESB")

    // producer setup
    val producerProps = new Properties()
    producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)

    // consume stream
    val stream = env.addSource(new FlinkKafkaConsumer010[String](consumerTopic, new SimpleStringSchema(), consumerProps))
    stream.print()

    // produce stream
    FlinkKafkaProducer010.writeToKafkaWithTimestamps(stream, producerTopic, new SimpleStringSchema(), producerProps)

    val executionResult = env.execute("ESB - Flink Identity Query")
  }
}
