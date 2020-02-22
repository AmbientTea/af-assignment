package io.ambienttea.views.model

import akka.NotUsed
import akka.stream.IOResult
import akka.stream.scaladsl.{Flow, Keep, Sink}
import com.typesafe.scalalogging.StrictLogging
import io.ambienttea.views.model
import io.ambienttea.views.utils._

import scala.collection.mutable
import scala.concurrent.Future

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
    def clickThrough: Double = (clicks.toDouble * 100d / views.toDouble )

    def addClick(): CampaignStats = copy(clicks = clicks + 1)
    def addView(): CampaignStats = copy(views = views + 1)
    def addViewable(): CampaignStats = copy(viewableViews = viewableViews + 1)
  }

  def sink: Sink[ModelEvent, Future[IOResult]] = {
    Flow[ModelEvent]
      .fold(new Stats()) {
        case (stats, event) =>
          stats.add(event)
          stats
      }
      .map(Stats.encodeCSV)
      .mapConcat(_.toSeq)
      .toMat(fileSink("statistics.csv"))(Keep.right)
  }

  def encodeCSV(stats: Stats): mutable.Iterable[String] = {
    for ((campaignId, stats) <- stats.campaigns)
      yield {
        import stats._
        s"$campaignId,$views,$clicks,$viewableViews,$clickThrough"
      }

  }
}
