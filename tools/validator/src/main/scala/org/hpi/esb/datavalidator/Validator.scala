package org.hpi.esb.datavalidator

import org.hpi.esb.commons.config.Configs.QueryNames._
import org.hpi.esb.commons.config.Configs.{QueryConfig, benchmarkConfig}
import org.hpi.esb.commons.util.Logging
import org.hpi.esb.datavalidator.configuration.Config.validatorConfig
import org.hpi.esb.datavalidator.data.Record
import org.hpi.esb.datavalidator.kafka.TopicHandler
import org.hpi.esb.datavalidator.output.writers.ValidatorRunResultWriter
import org.hpi.esb.datavalidator.validation.{IdentityValidation, StatisticsValidation, Validation}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

class Validator() extends Logging {

  val startTime: Long = currentTimeInSecs()

  def execute(): Unit = {
    logger.info(s"Start time: $startTime")

    val topics = benchmarkConfig.topics
    val queryConfigs = benchmarkConfig.queryConfigs

    val topicHandlersByName = topics.map(topic => topic -> TopicHandler.create(topic, AkkaManager.system)).toMap

    val validationResults = getValidations(queryConfigs, topicHandlersByName)
      .map(_.execute())

    val resultWriter = new ValidatorRunResultWriter()

    validationResults.foreach(future => future.onComplete {
      case Success(results) => logger.info(s"Finshed ${results.query}")
      case Failure(e) => logger.error(e.getMessage)
    })

    Future.sequence(validationResults).onComplete({
      case Success(results) => resultWriter.outputResults(results, startTime); terminate()
      case Failure(e) => logger.error(e.getMessage); terminate()
    })
  }

  def terminate(): Unit = {
    AkkaManager.terminate()
  }

  def currentTimeInSecs(): Long = System.currentTimeMillis() / 1000

  def getValidations(queryConfigs: List[QueryConfig], topicHandlersByName: Map[String, TopicHandler]): List[Validation[_ <: Record]] = {
    queryConfigs.map {

      case QueryConfig(IdentityQuery, inputTopic, outputTopic) =>
        new IdentityValidation(topicHandlersByName(inputTopic), topicHandlersByName(outputTopic), AkkaManager.materializer)

      case QueryConfig(StatisticsQuery, inputTopic, outputTopic) =>
        new StatisticsValidation(topicHandlersByName(inputTopic), topicHandlersByName(outputTopic), validatorConfig.windowSize, AkkaManager.materializer)
    }
  }
}
