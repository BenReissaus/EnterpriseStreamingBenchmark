package org.hpi.esb.flink.kafka

import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010.writeToKafkaWithTimestamps
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.kafka.clients.producer.ProducerConfig

class KafkaProducer(producerTopic: String) extends KafkaConnector {

  props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)

  def produce(stream: DataStream[String]): Unit = {
    val config = writeToKafkaWithTimestamps(stream.javaStream, producerTopic, new SimpleStringSchema(), props)
    config.setWriteTimestampToKafka(true)
  }
}
