package io.ambienttea.views.model

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Sink}
import com.typesafe.scalalogging.StrictLogging
import io.ambienttea.views.model

import scala.collection.mutable

class Stats extends StrictLogging {
  val campaigns = new mutable.HashMap[CampaignId, Stats.CampaignStats]()

  def add(event: ModelEvent) =
    event match {
      case c: Click =>
        val stats = getStats(c.campaignId)
        putStats(stats.addClick)
      case v: View =>
        val stats = getStats(v.campaignId)
        putStats(stats.addView)
      case ViewableView(v) =>
        val stats = getStats(v.campaignId)
        putStats(stats.addViewable)
      case vc: ViewWithClick =>
        logger.warn(s"stats should not receive $vc")
      case ve: ViewableViewEvent =>
        logger.warn(s"stats should not receive $ve")
    }

  private def getStats(id: CampaignId): Stats.CampaignStats =
    campaigns.getOrElseUpdate(id, model.Stats.CampaignStats(id))

  private def putStats(stats: Stats.CampaignStats) = {
    campaigns.put(stats.id, stats)
  }
}

object Stats {
  case class CampaignStats(
      id: CampaignId,
      views: Long = 0,
      clicks: Long = 0,
      viewableViews: Long = 0
  ) {
    def clickThrough: CampaignId = (clicks * 100 / views)

    def addClick(): CampaignStats = copy(clicks = clicks + 1)
    def addView(): CampaignStats = copy(views = views + 1)
    def addViewable(): CampaignStats = copy(viewableViews = viewableViews + 1)
  }

  def sink: Flow[ModelEvent, Stats, NotUsed] = {
    Flow[ModelEvent].fold(new Stats()) {
      case (stats, event) =>
        stats.add(event)
        stats
    }
  }
}
