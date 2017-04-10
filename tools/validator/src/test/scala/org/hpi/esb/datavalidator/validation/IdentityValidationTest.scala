package org.hpi.esb.datavalidator.validation

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{RunnableGraph, Source}
import org.hpi.esb.datavalidator.data.SimpleRecord
import org.hpi.esb.datavalidator.kafka.TopicHandler
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{AsyncFunSuite, BeforeAndAfter, FunSuite}

import scala.concurrent.ExecutionContext.Implicits.global

trait IdentityValidationTest {

  val inTopic = "IN"
  val outTopic = "OUT"

  val valueTimestamps: List[(Long, String)] = List[(Long, String)](
    (1, "1"), (500, "2"),  // first window
    (1000, "3"),(1001, "4"),(1050, "5"))  // second window

  val inCorrectValueTimestamps: List[(Long, String)] = List[(Long, String)](
    (999, "999"), (999, "999"),(999, "999"),(999, "999"),(999, "999"))


  implicit val system: ActorSystem = ActorSystem("ESBValidator")
  implicit val materializer: ActorMaterializer = ActorMaterializer()(system)
}

class IdentityValidationTestAsync extends AsyncFunSuite with IdentityValidationTest with ValidationTestHelpers with BeforeAndAfter with MockitoSugar {

  test("testCreateSink - correctness and response time fulfilled ") {
    val sink = new IdentityValidation(mock[TopicHandler], mock[TopicHandler], materializer).createSink()

    val inSimpleRecords = valueTimestamps.map { case (timestamp, value) => Some(SimpleRecord.deserialize(value, timestamp)) }
    val outSimpleRecords = inSimpleRecords
    val testSource = Source(inSimpleRecords.zip(outSimpleRecords))

    val graph = combineSourceWithSink[SimpleRecord](testSource, sink)
    val validationResult = RunnableGraph.fromGraph(graph).run()

    validationResult.map({ result => assert(result.fulfillsConstraints()) })
  }

  test("testCreateSink - incorrect results") {
    val sink = new IdentityValidation(mock[TopicHandler], mock[TopicHandler], materializer).createSink()

    val inSimpleRecords = createSimpleRecordsList(valueTimestamps)
    val outSimpleRecords = createSimpleRecordsList(inCorrectValueTimestamps)
    val testSource = Source(inSimpleRecords.zip(outSimpleRecords))

    val graph = combineSourceWithSink[SimpleRecord](testSource, sink)
    val validationResult = RunnableGraph.fromGraph(graph).run()

    validationResult.map(result => assert(!result.fulfillsConstraints()))
  }

  test("testCreateSink - too few results") {
    val sink = new IdentityValidation(mock[TopicHandler], mock[TopicHandler], materializer).createSink()

    val inSimpleRecords = createSimpleRecordsList(valueTimestamps)
    val outSimpleRecords = createSimpleRecordsList(valueTimestamps.dropRight(1))
    val testSource = Source(inSimpleRecords.zipAll(outSimpleRecords, None, None))

    val graph = combineSourceWithSink[SimpleRecord](testSource, sink)
    val validationResult = RunnableGraph.fromGraph(graph).run()

    validationResult.map({ result =>
      assert(!result.fulfillsConstraints() && result.correctness.getResultMessage.contains("Too few"))
    })
  }

  test("testCreateSink - too many results") {
    val sink = new IdentityValidation(mock[TopicHandler], mock[TopicHandler], materializer).createSink()

    val inSimpleRecords = createSimpleRecordsList(valueTimestamps.dropRight(1))
    val outSimpleRecords = createSimpleRecordsList(valueTimestamps)
    val testSource = Source(inSimpleRecords.zipAll(outSimpleRecords, None, None))

    val graph = combineSourceWithSink[SimpleRecord](testSource, sink)
    val validationResult = RunnableGraph.fromGraph(graph).run()

    validationResult.map({ result =>
      assert(!result.fulfillsConstraints() && result.correctness.getResultMessage.contains("Too many"))
    })
  }

  test("testCreateSink - correct response time calculation") {

    val sink = new IdentityValidation(mock[TopicHandler], mock[TopicHandler], materializer).createSink()

    val responseTime = 100
    val inValues = createSimpleRecordsList(valueTimestamps)
    val outValues = createSimpleRecordsList(valueTimestamps.map { case (timestamp, value) => (timestamp + responseTime, value) })
    val testSource = Source(inValues.zip(outValues))

    val graph = combineSourceWithSink[SimpleRecord](testSource, sink)
    val validationResult = RunnableGraph.fromGraph(graph).run()

    val expectedResponseTimes = Array.fill(valueTimestamps.length)(responseTime)
    validationResult.map(result => assert(result.fulfillsConstraints() && result.responseTime.getAllValues.sameElements(expectedResponseTimes)))
  }
}

class IdentityValidationTestSync extends FunSuite with IdentityValidationTest with ValidationTestHelpers with BeforeAndAfter with MockitoSugar {

  test("testCreateSource - successful") {

    val inTopicHandler = createTopicHandler(inTopic, valueTimestamps)
    val outTopicHandler = createTopicHandler(outTopic, valueTimestamps)

    val source = new IdentityValidation(inTopicHandler, outTopicHandler, materializer).createSource()

    val graph = addTestSink[SimpleRecord](source, system)
    val validationResult = RunnableGraph.fromGraph(graph).run()

    validationResult.request(valueTimestamps.length)

    valueTimestamps.foreach { case (_,value) => validationResult.expectNext((Some(SimpleRecord(value.toLong)()), Some(SimpleRecord(value.toLong)())))}
    validationResult.expectComplete()
  }

  test("testCreateSource - unserializable sensor values") {
    val unserializableValueTimestamps: List[(Long, String)] = List[(Long, String)]((1, "notserializable"), (500, "notserializable"))
    val inTopicHandler = createTopicHandler(inTopic, unserializableValueTimestamps)
    val outTopicHandler = createTopicHandler(outTopic, valueTimestamps)

    val source = new IdentityValidation(inTopicHandler, outTopicHandler, materializer)
      .createSource()

    val graph = addTestSink[SimpleRecord](source, system)
    val validationResult = RunnableGraph.fromGraph(graph).run()

    validationResult.request(unserializableValueTimestamps.length)
    validationResult.expectError()
  }
}

