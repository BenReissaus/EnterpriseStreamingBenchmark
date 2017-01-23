package org.hpi.esb.flink

import org.hpi.esb.flink.kafka.KafkaConsumer
import org.hpi.esb.flink.kafka.KafkaProducer
import org.hpi.esb.flink.query.Query

class Pipeline(consumer: KafkaConsumer, query: Query[String, String], producer: KafkaProducer) {

  def execute() = {
    val stream = consumer.consume()
    val resultStream = query.execute(stream)
    producer.produce(resultStream)
  }
}
