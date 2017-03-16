package org.hpi.esb.datavalidator

import org.hpi.esb.commons.config.Configs.QueryNames._
import org.hpi.esb.commons.config.Configs.{QueryConfig, benchmarkConfig}
import org.hpi.esb.datavalidator.config.Configurable
import org.hpi.esb.datavalidator.consumer.Consumer
import org.hpi.esb.datavalidator.util.Logging
import org.hpi.esb.datavalidator.validation.{IdentityValidation, StatisticsValidation}

class Validator() extends Configurable with Logging {

  def execute(): Unit = {

    val consumer = new Consumer(benchmarkConfig.getAllTopics, config.consumer)
    val records = consumer.consume()

    val validations = benchmarkConfig.streamConfigs.flatMap(streamConfig => {

      val sourceResults = records.getTopicResults(streamConfig.sourceName)
      streamConfig.queryConfigs.map {

        case QueryConfig(IdentityQuery, sinkName) =>
          val sinkResults = records.getTopicResults(sinkName)
          new IdentityValidation(sourceResults, sinkResults)

        case QueryConfig(StatisticsQuery, sinkName) =>
          val sinkResults = records.getTopicResults(sinkName)
          new StatisticsValidation(sourceResults, sinkResults, config.windowSize)
      }
    })

    validations.foreach(_.fulfillsRequirements())
  }
}
