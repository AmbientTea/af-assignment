package io.ambienttea.views

import java.text.SimpleDateFormat

package object model {
  type CampaignId = Long

  /* This needs to be a function returning a fresh instance,
   * because `SimpleDateFormat` is not thread-safe.
   */
  def dateFormat() = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
}
