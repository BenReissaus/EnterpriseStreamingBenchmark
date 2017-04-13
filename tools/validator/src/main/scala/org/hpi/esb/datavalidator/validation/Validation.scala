package org.hpi.esb.datavalidator.validation

import akka.NotUsed
import akka.stream.scaladsl.{Flow, GraphDSL, RunnableGraph, Sink}
import akka.stream.{ActorMaterializer, ClosedShape, Graph, SourceShape}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.hpi.esb.datavalidator.data.{Record, SimpleRecord, Statistics}
import org.hpi.esb.datavalidator.kafka.TopicHandler
import org.hpi.esb.datavalidator.metrics.CorrectnessMessages._
import org.hpi.esb.datavalidator.util.Logging

import scala.concurrent.Future

abstract class Validation[T <: Record](inTopicHandler: TopicHandler,
                                       outTopicHandler: TopicHandler,
                                       materializer: ActorMaterializer) extends Logging {


  val valueName: String
  val queryName: String

  val inNumberOfMessages: Long = inTopicHandler.numberOfMessages
  val outNumberOfMessages: Long = outTopicHandler.numberOfMessages

  val take: Long => Flow[ConsumerRecord[String, String], ConsumerRecord[String, String], NotUsed]
  = (numberOfMessages: Long) => Flow[ConsumerRecord[String, String]].take(numberOfMessages)

  val toSimpleRecords = Flow[ConsumerRecord[String, String]]
    .map(record => SimpleRecord.deserialize(record.value(), record.timestamp()))

  val toStatistics = Flow[ConsumerRecord[String, String]]
    .map(record => Statistics.deserialize(record.value(), record.timestamp()))

  def execute(): Future[ValidationResult] = {

    val partialSource = createSource()
    val partialSink = createSink()

    val graph = GraphDSL.create(partialSink) { implicit builder =>
      sink =>
        import GraphDSL.Implicits._
        val source = builder.add(partialSource)

        source ~> sink

        ClosedShape
    }

    val runnableGraph = RunnableGraph.fromGraph(graph)
    runnableGraph.run()(materializer)
  }

  def createSink(): Sink[(Option[T], Option[T]), Future[ValidationResult]] = {

    Sink.fold[ValidationResult, (Option[T], Option[T])](new ValidationResult(queryName, inTopicHandler.topicName)) {
      case (validationResult, pair) => updateAndGetValidationResult(validationResult, pair)
    }
  }

  def updateAndGetValidationResult(validationResult: ValidationResult, pair: (Option[T], Option[T])): ValidationResult = {
    pair match {

      case (Some(expectedValue), (Some(actualValue))) =>
        validationResult.updateResponseTime(getResponseTime(expectedValue, actualValue))
        if (expectedValue != actualValue) {
          validationResult.updateCorrectness(isCorrect = false)
          logger.error(UNEQUAL_VALUES(inTopicHandler.topicName, expectedValue.prettyPrint, actualValue.prettyPrint))
        }

      case (None, (Some(_))) =>
        validationResult.updateCorrectness(isCorrect = false)
        logger.error(TOO_MANY_VALUES_CREATED(inTopicHandler.topicName, valueName))

      case (Some(_), (None)) =>
        validationResult.updateCorrectness(isCorrect = false)
        logger.error(TOO_FEW_VALUES_CREATED(inTopicHandler.topicName, valueName))

      case _ =>
    }
    validationResult
  }

  def getResponseTime(inRecord: Record, outRecord: Record): Long = {
    outRecord.timestamp - inRecord.timestamp
  }

  def createSource(): Graph[SourceShape[(Option[T], Option[T])], NotUsed]

}
