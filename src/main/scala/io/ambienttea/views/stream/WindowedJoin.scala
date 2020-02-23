package io.ambienttea.views.stream

import java.time.{Duration, Instant}

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.{Flow, GraphDSL, MergeSorted, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic}
import io.ambienttea.views.model.{Click, OrderedBy, RelatedBy, View}

import scala.collection.mutable

object WindowedJoin {
  /* Joins stream elements by `relation` within a window
   * specified by the distance cut-off function `outOfWindow`.
   */
  def shape[T1, T2, C: Ordering, J](
      withinWindow: (C, C) => Boolean
  )(
      implicit
      relation: RelatedBy[T1, T2, J],
      ord1: OrderedBy[T1, C],
      ord2: OrderedBy[T2, C]
  ): Graph[FanInShape2[T1, T2, (T1, T2)], NotUsed] =
    GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      implicit val ord: Ordering[Either[T1, T2]] = Ordering.by {
        case Left(value)  => ord1.ord(value)
        case Right(value) => ord2.ord(value)
      }

      val in0 = b.add(Flow[T1].map(Left.apply))
      val in1 = b.add(Flow[T2].map(Right.apply))

      val window = new Window[T1, T2, C, J](withinWindow)

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
      withinWindow: (C, C) => Boolean
  )(
      implicit ord: Ordering[Either[T1, T2]],
      relation: RelatedBy[T1, T2, J],
      ord1: OrderedBy[T1, C],
      ord2: OrderedBy[T2, C]
  ) {
    import relation._
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

    def ord(e: Either[T1, T2]): C = e.fold(ord1.ord, ord2.ord)

    def removeOldest(newVal: Either[T1, T2]): Unit = {
      while (queue.nonEmpty && !withinWindow(ord(newVal), ord(queue.head))) {
        queue.dequeue()
      }
    }
  }
}
