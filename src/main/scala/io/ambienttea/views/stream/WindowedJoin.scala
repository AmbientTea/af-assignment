package io.ambienttea.views.stream

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.{GraphDSL, MergeSorted, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic}
import io.ambienttea.views.model.{Click, View}

import scala.collection.mutable

object WindowedJoin {
  /* Joins stream elements by the key produced by `join1` and `join2` within a window
   * of size `maxWindowSize`.
   */
  def apply[T1, T2, C: Ordering, J](
      source: Source[Either[T1, T2], _],
      join1: T1 => J,
      join2: T2 => J,
      maxWindowSize: Int
  ): Source[(T1, T2), _] = {
    val window = new Window[T1, T2, J](join1, join2, maxWindowSize)
    source.mapConcat(window.push)
  }

  def shape[T1, T2, C: Ordering, J](
      source: Source[Either[T1, T2], _],
      join1: T1 => J,
      join2: T2 => J,
      cmp1: T1 => C,
      cmp2: T2 => C,
      maxWindowSize: Int
  ): Graph[FanInShape2[Either[T1, T2], Either[T1, T2], (T1, T2)], NotUsed] =
    GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      implicit val ord: Ordering[Either[T1, T2]] = Ordering.by {
        case Left(value) => cmp1(value)
        case Right(value) => cmp2(value)
      }

      val window = new Window[T1, T2, J](join1, join2, maxWindowSize)

      val merge = b.add(new MergeSorted[Either[T1, T2]]())
      val joined = merge.out.mapConcat(window.push)

      new FanInShape2(merge.in0, merge.in1, joined.outlet)
    }

  class Window[T1, T2, J](
      join1: T1 => J,
      join2: T2 => J,
      maxWindowSize: Int = 100
  ) {
    val queue = new mutable.ArrayDeque[Either[T1, T2]]()
    val lefts = new mutable.HashMap[J, T1]()
    val rights = new mutable.HashMap[J, T2]()

    def push(newVal: Either[T1, T2]): Seq[(T1, T2)] = {
      removeOldest()

      queue.prepend(newVal)

      (newVal match {
        case Left(l) =>
          val j = join1(l)
          lefts(j) = l
          rights.get(j).map(l -> _)
        case Right(r) =>
          val j = join2(r)
          rights(j) = r
          lefts.get(j).map(_ -> r)
      }).toSeq
    }

    def removeOldest(): Unit = {
      while (queue.nonEmpty && queue.size >= maxWindowSize) {
        queue.removeLast() match {
          case Left(value)  => lefts.remove(join1(value))
          case Right(value) => rights.remove(join2(value))
        }
      }
    }
  }
}
