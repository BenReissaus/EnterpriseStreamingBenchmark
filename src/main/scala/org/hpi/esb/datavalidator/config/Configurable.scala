package org.hpi.esb.datavalidator.config

import pureconfig.loadConfig

import scala.util.{Failure, Success}

case class ValidatorConfig(consumer: KafkaConsumerConfig, topics: TopicConfig, windowSize: Long)

case class KafkaConsumerConfig(bootstrapServers: String, autoCommit: String,
                               autoCommitInterval: String, sessionTimeout: String,
                               keyDeserializerClass: String, valueDeserializerClass: String)

case class TopicConfig(in: String, out: String, stats: String, index: String) {
  val inTopic: String = getTopic(in, index)
  val outTopic: String = getTopic(out, index)
  val statsTopic: String = getTopic(stats, index)
  val all: List[String] = List(inTopic, outTopic, statsTopic)

  def getTopic(topic: String, index: String): String = {
    s"${topic}_$index"
  }
}

trait Configurable {

  val config: ValidatorConfig = loadConfig[ValidatorConfig] match {
    case Failure(f) => throw f
    case Success(conf) => conf
  }
}

