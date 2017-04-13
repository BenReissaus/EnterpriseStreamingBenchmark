package org.hpi.esb.datavalidator.validation

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.{Flow, GraphDSL}
import org.hpi.esb.datavalidator.config.Configurable
import org.hpi.esb.datavalidator.data.{SimpleRecord, Statistics}
import org.hpi.esb.datavalidator.kafka.TopicHandler
import org.hpi.esb.datavalidator.util.Logging
import org.hpi.esb.datavalidator.validation.graphstage.{AccumulateWhileUnchanged, IgnoreLastElement, ZipWhileEitherAvailable}

class StatisticsValidation(inTopicHandler: TopicHandler,
                           outTopicHandler: TopicHandler, windowSize: Long,
                           materializer: ActorMaterializer)
  extends Validation[Statistics](inTopicHandler, outTopicHandler, materializer) with Configurable with Logging {

  override val valueName = "Statistics"
  override val queryName = "Statistics Query"


  val collectByWindow = new AccumulateWhileUnchanged[SimpleRecord, Long](r => windowStart(r.timestamp))
  val calculateStatistics = Flow[Seq[SimpleRecord]].map(s =>
    s.foldLeft(new Statistics()())((stats, record) => stats.getUpdatedWithValue(record.timestamp, record.value)))


  def createSource(): Graph[SourceShape[(Option[Statistics], Option[Statistics])], NotUsed] = {

    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val zip = builder.add(ZipWhileEitherAvailable[Statistics]())
      val ignoreLast = builder.add(new IgnoreLastElement[(Option[Statistics], Option[Statistics])]())

      inTopicHandler.topicSource ~> take(inNumberOfMessages) ~> toSimpleRecords ~> collectByWindow ~> calculateStatistics  ~> zip.in0
      outTopicHandler.topicSource ~> take(outNumberOfMessages) ~> toStatistics ~> zip.in1
      zip.out ~> ignoreLast

      SourceShape(ignoreLast.out)
    }
  }

  def windowStart(timestamp: Long): Long = {
    timestamp - (timestamp % windowSize)
  }
}
