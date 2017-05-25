package org.hpi.esb.datavalidator.validation

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{RunnableGraph, Source}
import org.hpi.esb.datavalidator.data.Statistics
import org.hpi.esb.datavalidator.kafka.TopicHandler
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{AsyncFunSuite, BeforeAndAfter, FunSuite}

import scala.concurrent.ExecutionContext.Implicits.global

trait StatisticsValidationTest {
  implicit val system: ActorSystem = ActorSystem("ESBValidator")
  implicit val materializer: ActorMaterializer = ActorMaterializer()(system)

  val inTopic = "IN"
  val statsTopic = "STATS"
  val windowSize = 1000

  val valueTimestamps: List[(Long, String)] = List[(Long, String)](
    (1, "1"), (500, "2"), // first window
    (1000, "3"), (1001, "4"), (1050, "5") // second window
  )

  val correctResultStats = List[(Long, String)](
    (2000, "1,2,3,2,1.5"),
    (3000, "3,5,12,3,4")
  )

  val inCorrectResultStats = List[(Long, String)](
    (999, "999,999,999,999,999"),
    (3000, "999,999,999,999,999")
  )
}

class StatisticsValidationTestAsync extends AsyncFunSuite with StatisticsValidationTest
  with ValidationTestHelpers with BeforeAndAfter with MockitoSugar {

  test("testCreateSink - correctness and response time fulfilled ") {
    val sink = new StatisticsValidation(mock[TopicHandler], mock[TopicHandler], windowSize, materializer).createSink()

    val inStatistics = correctResultStats.map { case (timestamp, value) => Some(Statistics.deserialize(value, timestamp)) }
    val outStatistics = inStatistics
    val testSource = Source(inStatistics.zip(outStatistics))

    val graph = combineSourceWithSink[Statistics](testSource, sink)
    val validationResult = RunnableGraph.fromGraph(graph).run()

    validationResult.map({ result => assert(result.fulfillsConstraints()) })
  }

  test("testCreateSink - incorrect results") {
    val sink = new StatisticsValidation(mock[TopicHandler], mock[TopicHandler], windowSize, materializer).createSink()

    val inStatistics = createStatisticsList(correctResultStats)
    val outStatistics = createStatisticsList(inCorrectResultStats)
    val testSource = Source(inStatistics.zip(outStatistics))

    val graph = combineSourceWithSink[Statistics](testSource, sink)
    val validationResult = RunnableGraph.fromGraph(graph).run()

    validationResult.map(result => assert(!result.fulfillsConstraints()))
  }

  test("testCreateSink - too few statistics results") {
    val sink = new StatisticsValidation(mock[TopicHandler], mock[TopicHandler], windowSize, materializer).createSink()

    val inStatistics = createStatisticsList(correctResultStats)
    val outStatistics = createStatisticsList(correctResultStats.dropRight(1))
    val testSource = Source(inStatistics.zipAll(outStatistics, None, None))

    val graph = combineSourceWithSink[Statistics](testSource, sink)
    val validationResult = RunnableGraph.fromGraph(graph).run()

    validationResult.map({ result =>
      assert(!result.fulfillsConstraints())
    })
  }

  test("testCreateSink - too many statistics results") {
    val sink = new StatisticsValidation(mock[TopicHandler], mock[TopicHandler], windowSize, materializer).createSink()

    val inStatistics = createStatisticsList(correctResultStats.dropRight(1))
    val outStatistics = createStatisticsList(correctResultStats)
    val testSource = Source(inStatistics.zipAll(outStatistics, None, None))

    val graph = combineSourceWithSink[Statistics](testSource, sink)
    val validationResult = RunnableGraph.fromGraph(graph).run()

    validationResult.map({ result =>
      assert(!result.fulfillsConstraints())
    })
  }

  test("testCreateSink - correct response time calculation") {
    val sink = new StatisticsValidation(mock[TopicHandler], mock[TopicHandler], windowSize, materializer).createSink()

    val responseTime = 100
    val inStatistics = createStatisticsList(correctResultStats)
    val outStatistics = createStatisticsList(correctResultStats.map {
      case (timestamp, value) => (timestamp + responseTime, value)
    })
    val testSource = Source(inStatistics.zip(outStatistics))

    val graph = combineSourceWithSink[Statistics](testSource, sink)
    val validationResult = RunnableGraph.fromGraph(graph).run()

    val expectedResponseTimes = Array.fill(correctResultStats.length)(responseTime)
    validationResult.map({ result =>
      assert(result.fulfillsConstraints() && result.responseTime.getAllValues.sameElements(expectedResponseTimes))
    })
  }
}

class StatisticsValidationTestSync extends FunSuite with StatisticsValidationTest
  with ValidationTestHelpers with BeforeAndAfter with MockitoSugar {

  val numberOfElements = 1000

  test("testCreateSource - successful") {

    val inTopicHandler = createTopicHandler(inTopic, valueTimestamps)
    val outTopicHandler = createTopicHandler(statsTopic, correctResultStats)
    val source = new StatisticsValidation(inTopicHandler, outTopicHandler, windowSize, materializer).createSource()

    val graph = addTestSink[Statistics](source, system)
    val validationResult = RunnableGraph.fromGraph(graph).run()

    validationResult.request(numberOfElements)
    correctResultStats.dropRight(1).foreach {
      case (timestamp, value) => val stat = Some(Statistics.deserialize(value, timestamp))
        validationResult.expectNext((stat, stat))
    }
    validationResult.expectComplete()
  }

  test("testCreateSource - unserializable sensor values") {
    val unserializableValueTimestamps: List[(Long, String)] = List[(Long, String)]((1, "notserializable"), (500, "notserializable"))
    val inTopicHandler = createTopicHandler(inTopic, unserializableValueTimestamps)
    val outTopicHandler = createTopicHandler(statsTopic, correctResultStats)
    val source = new StatisticsValidation(inTopicHandler, outTopicHandler, windowSize, materializer).createSource()

    val graph = addTestSink[Statistics](source, system)
    val validationResult = RunnableGraph.fromGraph(graph).run()

    validationResult.request(numberOfElements)
    validationResult.expectError()
  }

  test("testCreateSource - unserializable statistics values") {

    val inTopicHandler = createTopicHandler(inTopic, valueTimestamps)
    val unserializableStats: List[(Long, String)] = List[(Long, String)]((1, "notserializable"), (500, "notserializable"))
    val outTopicHandler = createTopicHandler(statsTopic, unserializableStats)
    val source = new StatisticsValidation(inTopicHandler, outTopicHandler, windowSize, materializer).createSource()

    val graph = addTestSink[Statistics](source, system)
    val validationResult = RunnableGraph.fromGraph(graph).run()

    validationResult.request(numberOfElements)
    validationResult.expectError()
  }
}
