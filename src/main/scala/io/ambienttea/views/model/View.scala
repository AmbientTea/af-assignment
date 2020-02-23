package io.ambienttea.views.model

import java.time.Instant
import scala.util.Try

case class View(id: View.Id, logtime: Instant, campaignId: CampaignId)
    extends ModelEvent

case class ViewableView(view: View) extends ModelEvent

object View {
  type Id = Long

  def decode(csv: String): Try[View] =
    Try {
      val Array(id, logtime, campaignId) = csv.split(",")
      val time = dateFormat.parse(logtime).toInstant
      View(id.toLong, time, campaignId.toLong)
    }

  implicit val orderedByInstant: OrderedBy[View, Instant] =
    new OrderedBy(_.logtime)
}

object ViewableView {
  def encodeCSV(view: ViewableView): String = {
    import view.view._
    s"$id,$logtime,$campaignId"
  }
}
