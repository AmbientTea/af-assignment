package io.ambienttea.views.stream

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import org.scalatest.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class WindowedJoinTest extends AsyncWordSpec with Matchers {

  implicit val ac: ActorSystem = ActorSystem()

  "WindowedJoin" should {
    val source: Source[Either[Int, Int], NotUsed] = Source(
      List(Left(1), Right(2), Left(3), Left(4), Right(5), Right(6))
    )

    "match elements according to the match functions" in {
      val joinFlow = WindowedJoin[Int, Int, Int, Int](
        source,
        (_: Int) * 2,
        identity(_: Int),
        maxWindowSize = 100
      )
      val expectedResult = List(1 -> 2, 3 -> 6)

      joinFlow.runWith(Sink.seq).map { s =>
        println(s"$s ?= $expectedResult")
        s should contain theSameElementsInOrderAs expectedResult
      }
    }

    "properly merge two sources when window size is smaller than their size" in {
      val joinFlow = WindowedJoin[Int, Int, Int, Int](
        source,
        (_: Int) * 2,
        identity(_: Int),
        maxWindowSize = 2
      )

      val expectedResult = List(1 -> 2)

      joinFlow.runWith(Sink.seq).map { s =>
        println(s"$s ?= $expectedResult")
        s should contain theSameElementsInOrderAs expectedResult
      }

    }
  }

}
