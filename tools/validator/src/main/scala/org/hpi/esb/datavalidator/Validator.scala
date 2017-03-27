package org.hpi.esb.datavalidator

import org.hpi.esb.commons.config.Configs.QueryNames._
import org.hpi.esb.commons.config.Configs.{QueryConfig, benchmarkConfig}
import org.hpi.esb.datavalidator.config.Configurable
import org.hpi.esb.datavalidator.consumer.{Consumer, Records}
import org.hpi.esb.datavalidator.util.Logging
import org.hpi.esb.datavalidator.validation.{IdentityValidation, StatisticsValidation, Validation}

class Validator() extends Configurable with Logging {

  def execute(): Unit = {

    val consumer = new Consumer(benchmarkConfig.getAllTopics, config.consumer)
    val records = consumer.consume()

    val queryConfigs = benchmarkConfig.queryConfigs

    val validationResults = getValidations(queryConfigs, records)
      .map(_.execute())

    validationResults.foreach(logger.info(_))
  }

  def getValidations(queryConfigs: List[QueryConfig], records: Records): List[Validation] = {
    queryConfigs.map {

      case QueryConfig(sourceName, sinkName, IdentityQuery) =>
        new IdentityValidation(records.getTopicResults(sourceName), records.getTopicResults(sinkName))

      case QueryConfig(sourceName, sinkName, StatisticsQuery) =>
        new StatisticsValidation(records.getTopicResults(sourceName), records.getTopicResults(sinkName), config.windowSize)
    }
  }
}
