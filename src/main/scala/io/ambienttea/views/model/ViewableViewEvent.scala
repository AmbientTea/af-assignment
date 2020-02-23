package io.ambienttea.views.model

import java.time.Instant

import scala.util.Try

case class ViewableViewEvent(
    id: ViewableViewEvent.Id,
    logtime: Instant,
    interactionId: View.Id
) extends ModelEvent

object ViewableViewEvent {
  type Id = Long

  def decode(csv: String): Try[ViewableViewEvent] =
    Try {
      val Array(id, logtime, intId) = csv.split(",")
      val time = dateFormat.parse(logtime).toInstant
      ViewableViewEvent(id.toLong, time, intId.toLong)
    }

  implicit val orderedByInstant: OrderedBy[ViewableViewEvent, Instant] =
    new OrderedBy(_.logtime)

  implicit val relatedToView: RelatedBy[View, ViewableViewEvent, View.Id] =
    new RelatedBy(_.id, _.interactionId)
}
