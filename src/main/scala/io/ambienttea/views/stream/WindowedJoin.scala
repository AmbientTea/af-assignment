package io.ambienttea.views.stream

import java.time.{Duration, Instant}

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.{Flow, GraphDSL, MergeSorted, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic}
import io.ambienttea.views.model.{Click, View}

import scala.collection.mutable

object WindowedJoin {
  /* Joins stream elements by the key produced by `join1` and `join2` within a window
   * specified by the distance cut-off function `outOfWindow`.
   */
  def shape[T1, T2, C: Ordering, J](
      join1: T1 => J,
      join2: T2 => J,
      cmp1: T1 => C,
      cmp2: T2 => C,
      withinWindow: (C, C) => Boolean
  ): Graph[FanInShape2[T1, T2, (T1, T2)], NotUsed] =
    GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      implicit val ord: Ordering[Either[T1, T2]] = Ordering.by {
        case Left(value)  => cmp1(value)
        case Right(value) => cmp2(value)
      }

      val in0 = b.add(Flow[T1].map(Left.apply))
      val in1 = b.add(Flow[T2].map(Right.apply))

      val window =
        new Window[T1, T2, C, J](join1, join2, cmp1, cmp2, withinWindow)

      val merge = b.add(new MergeSorted[Either[T1, T2]]())

      in0 ~> merge.in0
      in1 ~> merge.in1
      val joined = merge.out.mapConcat(window.push)

      new FanInShape2(in0.in, in1.in, joined.outlet)
    }

  def withinNMinutes(m: Int)(i1: Instant, i2: Instant): Boolean =
    Duration.between(i1, i2).toMinutes <= m

  /* A generic sliding window. Emits matching pairs when new element is added.
   * Uses `ord1 withinWindow ord2` to determine whether to prune the internal queue.
   */
  class Window[T1, T2, C: Ordering, J](
      join1: T1 => J,
      join2: T2 => J,
      ord1: T1 => C,
      ord2: T2 => C,
      withinWindow: (C, C) => Boolean
  )(implicit ord: Ordering[Either[T1, T2]]) {
    val queue = new mutable.PriorityQueue[Either[T1, T2]]()
    val lefts = new mutable.HashMap[J, T1]()
    val rights = new mutable.HashMap[J, T2]()

    def push(newVal: Either[T1, T2]): Seq[(T1, T2)] = {
      removeOldest(newVal)

      queue.addOne(newVal)

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

    def ord(e: Either[T1, T2]): C = e.fold(ord1, ord2)

    def removeOldest(newVal: Either[T1, T2]): Unit = {
      while (queue.nonEmpty && !withinWindow(ord(newVal), ord(queue.head))) {
        queue.dequeue()
      }
    }
  }
}
