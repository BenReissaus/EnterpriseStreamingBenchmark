package org.hpi.esb.datavalidator.consumer

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.hpi.esb.datavalidator.config.KafkaConsumerConfig
import org.hpi.esb.datavalidator.util.Logging

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class ConsumerLoop(topic: String, config: KafkaConsumerConfig, results: ListBuffer[ConsumerRecord[String, String]]) extends Runnable with Logging {

  private val props = createConsumerProps()
  private val consumer = new KafkaConsumer(props)

  initializeConsumer()

  override def run(): Unit = {

    var running = true
    var zeroCount = 0

    while (running) {
      val records = consumer.poll(1000).asInstanceOf[ConsumerRecords[String, String]]

      if (records.count() == 0) {
        logger.debug(s"Received 0 records from Kafka.")
        zeroCount += 1
        if (zeroCount == 3) {
          logger.debug("Received 0 records from Kafka for the third time. We assume the stream has finished and terminate.")
          running = false
        }
      }

      for (record <- records) {
        results.append(record)
      }
    }
    consumer.close()
  }

  private def initializeConsumer(): Unit = {
    val topicPartitions = List(new TopicPartition(topic, 0))
    consumer.assign(topicPartitions)
    consumer.seekToBeginning(topicPartitions)
  }

  private def createConsumerProps(): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, s"Validator")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, config.autoCommit)
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, config.autoCommitInterval)
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, config.sessionTimeout)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, config.keyDeserializerClass)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, config.valueDeserializerClass)
    props
  }
}
