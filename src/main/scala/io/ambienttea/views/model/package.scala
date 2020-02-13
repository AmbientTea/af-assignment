package io.ambienttea.views

import java.text.SimpleDateFormat

package object model {
  type CampaignId = Long

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
}
