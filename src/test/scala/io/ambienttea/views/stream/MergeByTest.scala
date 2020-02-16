package io.ambienttea.views.stream

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import org.scalatest.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class MergeByTest extends AsyncWordSpec with Matchers {
  implicit val as = ActorSystem()
  "MergeBy" should {
    "properly merge two streams" in {
      val stream1 = Source(Range(1, 100, 2))
      val stream2 = Source(Range(2, 100, 2)).map(_ * 10)
      val expectedResult = Range(1, 100)

      val result = MergeBy(stream1, stream2)(identity, _ / 10)
        .map {
          case Left(v)  => v
          case Right(v) => v / 10
        }
        .runWith(Sink.seq)

      result.map(seq =>
        seq should contain theSameElementsInOrderAs expectedResult
      )
    }
  }

}
