package org.hpi.esb.datavalidator

import org.hpi.esb.commons.config.Configs.QueryNames._
import org.hpi.esb.commons.config.Configs.{QueryConfig, benchmarkConfig}
import org.hpi.esb.datavalidator.config.Configurable
import org.hpi.esb.datavalidator.data.Record
import org.hpi.esb.datavalidator.kafka.TopicHandler
import org.hpi.esb.datavalidator.util.Logging
import org.hpi.esb.datavalidator.validation.{IdentityValidation, StatisticsValidation, Validation, ValidationResult}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

class Validator() extends Configurable with Logging {

  def execute(): Unit = {

    val topics = benchmarkConfig.getAllTopics
    val queryConfigs = benchmarkConfig.queryConfigs

    val topicHandlersByName = topics.map(topic => topic -> TopicHandler.create(topic, AkkaManager.system)).toMap

    val validationResults = getValidations(queryConfigs, topicHandlersByName)
      .map(_.execute())

    Future.sequence(validationResults).onComplete({
      case Success(results) => results.foreach(printResult); AkkaManager.terminate()
      case Failure(e) => logger.error(e.getMessage); AkkaManager.terminate()
    })
  }


  def printResult(result: ValidationResult): Unit = {
    logger.info(result)
  }

  def getValidations(queryConfigs: List[QueryConfig], topicHandlersByName: Map[String, TopicHandler]): List[Validation[_ <: Record]] = {
    queryConfigs.map {

      case QueryConfig(sourceName, sinkName, IdentityQuery) =>
        new IdentityValidation(topicHandlersByName(sourceName), topicHandlersByName(sinkName), AkkaManager.materializer)

      case QueryConfig(sourceName, sinkName, StatisticsQuery) =>
        new StatisticsValidation(topicHandlersByName(sourceName), topicHandlersByName(sinkName), config.windowSize, AkkaManager.materializer)
    }
  }
}
