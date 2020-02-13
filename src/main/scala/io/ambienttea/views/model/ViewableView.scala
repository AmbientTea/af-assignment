package io.ambienttea.views.model

import java.time.Instant

import scala.util.Try

case class ViewableView(
    id: ViewableView.Id,
    logtime: Instant,
    interactionId: View.Id
)

object ViewableView {
  type Id = Long

  def decode(csv: String): Try[ViewableView] =
    Try {
      val Array(id, logtime, intId) = csv.split(",")
      val time = dateFormat.parse(logtime).toInstant
      ViewableView(id.toLong, time, intId.toLong)
    }
}
