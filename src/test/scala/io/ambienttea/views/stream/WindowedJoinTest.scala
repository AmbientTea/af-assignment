package io.ambienttea.views.stream

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import org.scalatest.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class WindowedJoinTest extends AsyncWordSpec with Matchers {

  implicit val ac: ActorSystem = ActorSystem()

  "WindowedMerge" should {
    val source1 = Source(List(1, 2, 3, 4))
    val source2 = Source(List(1, 2, 5, 6))

    "properly merge two sources" in {
      val mergedFlow =
        Pipeline(source1, source2)(identity, identity)(
          _ * 2,
          identity,
          100
        )
      val expectedResult = List(1 -> 2, 3 -> 6)

      mergedFlow.runWith(Sink.seq).map { s =>
        println(s"$s ?= $expectedResult")
        s should contain theSameElementsInOrderAs expectedResult
      }
    }

    "properly merge two sources when window size is smaller than their size" in {
      val mergedFlow =
        Pipeline(source1, source2)(identity, identity)(
          _ * 2,
          identity,
          maxWindowSize = 2
        )

      val expectedResult = List(1 -> 2)

      mergedFlow.runWith(Sink.seq).map { s =>
        println(s"$s ?= $expectedResult")
        s should contain theSameElementsInOrderAs expectedResult
      }

    }
  }

}
