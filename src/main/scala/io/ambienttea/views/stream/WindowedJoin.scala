package io.ambienttea.views.stream

import akka.stream.scaladsl.Source

import scala.collection.mutable

object WindowedJoin {

  def apply[T1, T2, C: Ordering, J](
      source1: Source[T1, _],
      source2: Source[T2, _]
  )(
      cmp1: T1 => C,
      cmp2: T2 => C
  )(
      join1: T1 => J,
      join2: T2 => J,
      maxWindowSize: Int
  ): Source[(T1, T2), _] = {
    val merged = MergeBy(source1, source2)(cmp1, cmp2)

    val window = new Window[T1, T2, J](join1, join2, maxWindowSize)
    merged.mapConcat(window.push)
  }

  /* Joins stream elements by the key produced by `join1` and `join2` within a window
   * of size `maxWindowSize`.
   */
  private class Window[T1, T2, J](
      join1: T1 => J,
      join2: T2 => J,
      maxWindowSize: Int
  ) {
    val queue = new mutable.ArrayDeque[Either[T1, T2]]()
    val lefts = new mutable.HashMap[J, T1]()
    val rights = new mutable.HashMap[J, T2]()

    def push(newVal: Either[T1, T2]): Seq[(T1, T2)] = {
      if (queue.size >= maxWindowSize)
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
      while (queue.size >= maxWindowSize) {
        val elem = queue.last
        queue.removeLast()
        elem match {
          case Left(value)  => lefts.remove(join1(value))
          case Right(value) => rights.remove(join2(value))
        }
      }
    }
  }
}
