package io.ambienttea.views

import java.text.SimpleDateFormat

package object model {
  trait ModelEvent

  type CampaignId = Long

  /* This needs to be a function returning a fresh instance,
   * because `SimpleDateFormat` is not thread-safe.
   * https://medium.com/@daveford/numberformatexception-multiple-points-when-parsing-date-650baa6829b6
   */
  def dateFormat() = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  class RelatedBy[T1, T2, J](val join1: T1 => J, val join2: T2 => J)

  class OrderedBy[T, C: Ordering](val ord: T => C)
}
