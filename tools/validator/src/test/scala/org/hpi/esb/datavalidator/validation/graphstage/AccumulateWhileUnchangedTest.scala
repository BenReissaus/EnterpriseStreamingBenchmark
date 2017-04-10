package org.hpi.esb.datavalidator.validation.graphstage

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Source}
import akka.stream.testkit.scaladsl.TestSink
import org.hpi.esb.datavalidator.data.SimpleRecord
import org.scalatest.FunSuite

import scala.collection.immutable


class AccumulateWhileUnchangedTest extends FunSuite {

  test("test") {
    val windowSize = 1000
    def windowStart(timestamp: Long): Long = {
      timestamp - (timestamp % windowSize)
    }

    implicit val system = ActorSystem()
    implicit val materializer: ActorMaterializer = ActorMaterializer()(system)
    val first = immutable.Seq.range(1, 999, 10).map(t => SimpleRecord(1)(t))
    val second = immutable.Seq.range(1000, 1999, 10).map(t => SimpleRecord(1)(t))

    val records = first ++ second

    val s = TestSink.probe[Seq[SimpleRecord]]

    val (_, sink) = Source(records)
      .via(new AccumulateWhileUnchanged(r => windowStart(r.timestamp)))
      .toMat(s)(Keep.both)
      .run()

    sink.request(3000)
    sink.expectNext(first, second)
    sink.expectComplete()
  }

}
