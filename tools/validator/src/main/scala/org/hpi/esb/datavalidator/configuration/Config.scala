package org.hpi.esb.datavalidator.configuration

import org.hpi.esb.commons.config.Configs
import pureconfig.loadConfig

import scala.util.{Failure, Success}

case class ValidatorConfig(consumer: KafkaConsumerConfig, windowSize: Long)

case class KafkaConsumerConfig(bootstrapServers: String, autoCommit: String,
                               autoCommitInterval: String, sessionTimeout: String,
                               keyDeserializerClass: String, valueDeserializerClass: String)

object Config {

  val relativeValidationPath = "/tools/validator/"
  val validationPath = System.getProperty("user.dir") + relativeValidationPath
  val resultsPath = s"$validationPath/results"

  def resultFileName(currentTime: String): String = s"${Configs.benchmarkConfig.topicPrefix}_" +
    s"${Configs.benchmarkConfig.benchmarkRun}_$currentTime.csv"

  val validatorConfig: ValidatorConfig = loadConfig[ValidatorConfig] match {
    case Failure(f) => throw f
    case Success(conf) => conf
  }
}

