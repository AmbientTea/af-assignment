package io.ambienttea.views

import java.text.SimpleDateFormat

package object model {
  type CampaignId = Long

  /* This needs to be a function returning a fresh instance,
   * because `SimpleDateFormat` is not thread-safe.
   * https://medium.com/@daveford/numberformatexception-multiple-points-when-parsing-date-650baa6829b6
   */
  def dateFormat() = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
}
