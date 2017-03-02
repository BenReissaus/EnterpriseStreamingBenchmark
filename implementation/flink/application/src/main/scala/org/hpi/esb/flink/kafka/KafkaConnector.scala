package org.hpi.esb.flink.kafka

import java.util.Properties

trait KafkaConnector {
  val props = new Properties()

  // TODO: read from configuration file
  val BOOTSTRAP_SERVERS = "192.168.30.208:9092,192.168.30.207:9092,192.168.30.141:9092"
}
