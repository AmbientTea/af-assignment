package io.ambienttea.views.stream

import akka.stream.scaladsl.Source

object Pipeline {
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
    WindowedJoin(merged, join1, join2, maxWindowSize)
  }
}
