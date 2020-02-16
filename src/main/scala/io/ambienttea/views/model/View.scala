package io.ambienttea.views.model

import java.time.Instant
import scala.util.Try

case class View(id: View.Id, logtime: Instant, campaignId: CampaignId)

object View {
  type Id = Long

  def encodeCSV(view: View): String = {
    import view._
    s"$id,$logtime,$campaignId"
  }

  def decode(csv: String): Try[View] =
    Try {
      val Array(id, logtime, campaignId) = csv.split(",")
      val time = dateFormat.parse(logtime).toInstant
      View(id.toLong, time, campaignId.toLong)
    }
}
