package org.hpi.esb.flink.kafka

import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010.writeToKafkaWithTimestamps
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.kafka.clients.producer.ProducerConfig

class KafkaProducer(producerTopic: String) extends KafkaConnector {

  val uuid: String = java.util.UUID.randomUUID.toString

  // TODO: read properties from file
  props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
  props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, s"$producerTopic - $uuid")

  def produce(stream: DataStream[String]): Unit = {
    val config = writeToKafkaWithTimestamps(stream.javaStream, producerTopic, new SimpleStringSchema(), props)
    config.setWriteTimestampToKafka(true)
  }
}
