package io.ambienttea.views.model

import java.time.Instant

import scala.util.Try

case class Click(
    id: Click.Id,
    logtime: Instant,
    campaignId: CampaignId,
    interactionId: View.Id
)

object Click {
  type Id = Long

  def decode(csv: String): Try[Click] =
    Try {
      val Array(id, logtime, campId, intId) = csv.split(",")
      val time = dateFormat.parse(logtime).toInstant
      Click(id.toLong, time, campId.toLong, intId.toLong)
    }
}