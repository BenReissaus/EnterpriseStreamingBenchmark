package org.hpi.esb.datavalidator.validation.graphstage

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Source}
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.{ActorMaterializer, ClosedShape}
import org.scalatest.FunSuite

import scala.collection.immutable

class ZipWhileEitherAvailableTest extends FunSuite {


  implicit val system = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer()(system)

  // number of elements that are requested of the streaming system;
  val numberOfElements = 10000

  def createGraph(source1: Source[Int, NotUsed], source2: Source[Int, NotUsed]) = {

    val sink = TestSink.probe[(Option[Int], Option[Int])]
    GraphDSL.create(sink) { implicit builder =>
      s =>
        import GraphDSL.Implicits._

        val zip = builder.add(new ZipWhileEitherAvailable[Int, Int, (Option[Int], Option[Int])](Tuple2.apply[Option[Int],Option[Int]]))
        val s1 = builder.add(source1)
        val s2 = builder.add(source2)

        s1 ~> zip.in0
        s2 ~> zip.in1

        zip.out ~> s
        ClosedShape
    }
  }

  test("equal number of 'in1' and 'in2' messages") {

    val in1 = immutable.Seq.range(1, 10)
    val in2 = immutable.Seq.range(1, 10)
    val source1 = Source(in1)
    val source2 = Source(in2)

    val result = RunnableGraph.fromGraph(createGraph(source1, source2)).run()
    result.request(numberOfElements)
    in1.zip(in2).foreach {
      case (v1, v2) => result.expectNext((Some(v1), Some(v2)))
    }
    result.expectComplete()
  }

  test("more messages in 'in1'") {

    val in1 = immutable.Seq.range(1, 10)
    val in2 = immutable.Seq.range(1, 20)
    val source1 = Source(in1)
    val source2 = Source(in2)

    val result = RunnableGraph.fromGraph(createGraph(source1, source2)).run()
    result.request(numberOfElements)
    val zipped = in1.zipAll(in2, -1, -1)
    zipped.foreach {
      case (v1, v2) if v1 < 0 => result.expectNext((None, Some(v2)))
      case (v1, v2) => result.expectNext((Some(v1), Some(v2)))
    }
    result.expectComplete()

  }

  test("more messages in 'in2'") {

    val in1 = immutable.Seq.range(1, 20)
    val in2 = immutable.Seq.range(1, 10)
    val source1 = Source(in1)
    val source2 = Source(in2)

    val result = RunnableGraph.fromGraph(createGraph(source1, source2)).run()
    result.request(numberOfElements)
    val zipped = in1.zipAll(in2, -1, -1)
    zipped.foreach {
      case (v1, v2) if v2 < 0 => result.expectNext((Some(v1), None))
      case (v1, v2) => result.expectNext((Some(v1), Some(v2)))
    }
    result.expectComplete()
  }

  test("empty 'in2'") {

    val in1 = immutable.Seq.range(1, 10)
    val in2 = immutable.Seq[Int]()
    val source1 = Source(in1)
    val source2 = Source(in2)

    val result = RunnableGraph.fromGraph(createGraph(source1, source2)).run()

    result.request(numberOfElements)
    val zipped = in1.zipAll(in2, -1, -1)
    zipped.foreach {
      case (v1, v2) => result.expectNext((Some(v1), None))
    }
    result.expectComplete()
  }

  test("empty 'in1'") {

    val in1 = immutable.Seq[Int]()
    val in2 = immutable.Seq.range(1, 10)
    val source1 = Source(in1)
    val source2 = Source(in2)

    val result = RunnableGraph.fromGraph(createGraph(source1, source2)).run()

    result.request(numberOfElements)
    val zipped = in1.zipAll(in2, -1, -1)
    zipped.foreach {
      case (v1, v2) => result.expectNext((None, Some(v2)))
    }
    result.expectComplete()
  }
}
