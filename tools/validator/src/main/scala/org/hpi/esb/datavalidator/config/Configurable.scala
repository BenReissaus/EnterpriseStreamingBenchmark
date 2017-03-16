package org.hpi.esb.datavalidator.config

import pureconfig.loadConfig

import scala.util.{Failure, Success}

case class ValidatorConfig(consumer: KafkaConsumerConfig, windowSize: Long)

case class KafkaConsumerConfig(bootstrapServers: String, autoCommit: String,
                               autoCommitInterval: String, sessionTimeout: String,
                               keyDeserializerClass: String, valueDeserializerClass: String)


trait Configurable {

  val config: ValidatorConfig = loadConfig[ValidatorConfig] match {
    case Failure(f) => throw f
    case Success(conf) => conf
  }
}

