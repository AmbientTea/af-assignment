package io.ambienttea.views.model

import java.time.Instant

case class ViewWithClick(id: View.Id, logtime: Instant, clickId: Click.Id)

object ViewWithClick {
  def fromViewAndClick(view: View, click: Click): ViewWithClick =
    ViewWithClick(view.id, view.logtime, click.id)

  def encodeCSV(v: ViewWithClick): String = {
    import v._
    s"$id,$logtime,$clickId"
  }
}
