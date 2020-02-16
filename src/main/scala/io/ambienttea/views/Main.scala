package io.ambienttea.views

import akka.actor.ActorSystem
import com.typesafe.scalalogging.LazyLogging
import io.ambienttea.views.model._
import io.ambienttea.views.stream.DecodeCSV._
import io.ambienttea.views.stream.Pipeline
import io.ambienttea.views.utils._

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global

object Main extends LazyLogging {
  def main(args: Array[String]) {
    logger.info(s"Server started with arguments: $args")
    val viewsFileName = args(0)
    val clicksFileName = args(1)
    val viewableViewsFileName = args(2)

    implicit val ac: ActorSystem = ActorSystem()

    val viewsSource = fileSource(viewsFileName)
      .decodeCSV(View.decode)

    val clicksSource = fileSource(clicksFileName)
      .decodeCSV(Click.decode)

    val viewableViewEventsSource = fileSource(viewableViewsFileName)
      .decodeCSV(ViewableViewEvent.decode)

    val viewsWithClicks =
      Pipeline(viewsSource, clicksSource)(_.logtime, _.logtime)(
        _.id,
        _.interactionId,
        100
      ).map {
        case (view, click) => ViewWithClick.fromViewAndClick(view, click)
      }

    val viewableViews =
      Pipeline(viewsSource, viewableViewEventsSource)(_.logtime, _.logtime)(
        _.id,
        _.interactionId,
        100
      ).map(_._1)
        .map(ViewableView.apply)

    for {
      _ <- viewsWithClicks
        .map(ViewWithClick.encodeCSV)
        .runWith(fileSink("ViewsWithClicks.csv"))
      _ <- viewableViews
        .map(ViewableView.encodeCSV)
        .runWith(fileSink("ViewableViews.csv"))
      _ <- ac.terminate()
    } yield ()
  }

}
