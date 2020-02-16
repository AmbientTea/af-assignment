package io.ambienttea.views.stream

import akka.stream.scaladsl.Source

object MergeBy {

  /* Merges two streams with priority given to elements with lower
   * values given by the `cmp1` and `cmp2` functions. Assuming the
   * streams are almost sorted this should make the resulting string
   * mostly sorted as well.
   *
   * Occasional misordering of elements could be mitigated by
   * a windowed sorting step.
   */
  def apply[T1, T2, C: Ordering](
      source1: Source[T1, _],
      source2: Source[T2, _]
  )(
      cmp1: T1 => C,
      cmp2: T2 => C
  ) = {
    implicit val ord: Ordering[Either[T1, T2]] = Ordering.by {
      case Left(v)  => cmp1(v)
      case Right(v) => cmp2(v)
    }
    source1.map(Left.apply) mergeSorted source2.map(Right.apply)
  }

}
