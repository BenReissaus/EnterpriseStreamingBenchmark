package org.hpi.esb.datavalidator

import java.text.SimpleDateFormat
import java.util.Date

import org.hpi.esb.commons.config.Configs.QueryNames._
import org.hpi.esb.commons.config.Configs.{QueryConfig, benchmarkConfig}
import org.hpi.esb.commons.output.{CSVOutput, Tabulator}
import org.hpi.esb.datavalidator.config.Configurable
import org.hpi.esb.datavalidator.data.Record
import org.hpi.esb.datavalidator.kafka.TopicHandler
import org.hpi.esb.datavalidator.util.Logging
import org.hpi.esb.datavalidator.validation.{IdentityValidation, StatisticsValidation, Validation, ValidationResult}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

class Validator() extends Configurable with Logging {

  val currentTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
  val resultFileName = s"$currentTime.csv"

  def execute(): Unit = {

    val topics = benchmarkConfig.topics
    val queryConfigs = benchmarkConfig.queryConfigs

    val topicHandlersByName = topics.map(topic => topic -> TopicHandler.create(topic, AkkaManager.system)).toMap

    val validationResults = getValidations(queryConfigs, topicHandlersByName)
      .map(_.execute())

    validationResults.foreach(future => future.onComplete {
      case Success(results) => logger.info(s"Finshed ${results.query}")
      case Failure(e) => logger.error(e.getMessage)
    })

    Future.sequence(validationResults).onComplete({
      case Success(results) => outputResults(results); AkkaManager.terminate()
      case Failure(e) => logger.error(e.getMessage); AkkaManager.terminate()
    })
  }

  def outputResults(results: List[ValidationResult]): Unit = {
    val rows = results.map(_.getMeasuredResults)
    val table = ValidationResult.getHeader :: rows

    CSVOutput.write(table, resultsPath, resultFileName)
    logger.info(Tabulator.format(table))
  }

  def getValidations(queryConfigs: List[QueryConfig], topicHandlersByName: Map[String, TopicHandler]): List[Validation[_ <: Record]] = {
    queryConfigs.map {

      case QueryConfig(IdentityQuery, inputTopic, outputTopic) =>
        new IdentityValidation(topicHandlersByName(inputTopic), topicHandlersByName(outputTopic), AkkaManager.materializer)

      case QueryConfig(StatisticsQuery, inputTopic, outputTopic) =>
        new StatisticsValidation(topicHandlersByName(inputTopic), topicHandlersByName(outputTopic), config.windowSize, AkkaManager.materializer)
    }
  }
}
