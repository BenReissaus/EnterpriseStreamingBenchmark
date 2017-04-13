package org.hpi.esb.datavalidator.config

import pureconfig.loadConfig

import scala.util.{Failure, Success}

case class ValidatorConfig(consumer: KafkaConsumerConfig, windowSize: Long)

case class KafkaConsumerConfig(bootstrapServers: String, autoCommit: String,
                               autoCommitInterval: String, sessionTimeout: String,
                               keyDeserializerClass: String, valueDeserializerClass: String)


trait Configurable {

  val relativeValidationPath = "/tools/validator/"
  val validationPath = System.getProperty("user.dir") + relativeValidationPath
  val resultsPath = s"$validationPath/results"

  val config: ValidatorConfig = loadConfig[ValidatorConfig] match {
    case Failure(f) => throw f
    case Success(conf) => conf
  }
}

