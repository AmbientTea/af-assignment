package io.ambienttea.views

import java.time.Instant

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ClosedShape, IOResult}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, MergeSorted, RunnableGraph, Sink}
import com.typesafe.scalalogging.LazyLogging
import io.ambienttea.views.model._
import io.ambienttea.views.stream.DecodeCSV._
import io.ambienttea.views.stream.Pipeline
import io.ambienttea.views.stream.WindowedJoin.Window
import io.ambienttea.views.utils._

import scala.concurrent.{ExecutionContext, Future}
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

    implicit val ord1: Ordering[Either[View, Click]] = Ordering.by {
      case Left(v) => v.logtime
      case Right(v) => v.logtime
    }
    implicit val ord2: Ordering[Either[View, ViewableViewEvent]] = Ordering.by {
      case Left(v) => v.logtime
      case Right(v) => v.logtime
    }

    val graph = RunnableGraph.fromGraph(GraphDSL.create() {
      implicit b =>
        import GraphDSL.Implicits._

        val clicksBc = b.add(Broadcast[Either[Nothing, Click]](2))
        val viewsBc = b.add(Broadcast[Either[View, Nothing]](3))

        val views = viewsSource.map(Left.apply)
        val clicks = clicksSource.map(Right.apply)
        val viewableViewEvents = viewableViewEventsSource.map(Right.apply)

        val viewsClicksMerge = b.add(new MergeSorted[Either[View, Click]]())
        val viewsEventsMerge =
          b.add(new MergeSorted[Either[View, ViewableViewEvent]]())

        clicks ~> clicksBc ~> viewsClicksMerge.in0
        clicksBc ~> Sink.ignore
        views ~> viewsBc ~> viewsClicksMerge.in1
        viewsBc ~> viewsEventsMerge.in0
        viewsBc ~> Sink.ignore
        viewableViewEvents ~> viewsEventsMerge.in1

        val viewsClicksWindow =
          new Window[View, Click, View.Id](_.id, _.interactionId)
        val viewsWithClicksCSV = viewsClicksMerge.out
          .mapConcat(viewsClicksWindow.push)
          .alsoTo(Sink.foreach(println(_)))
          .map { case (v, c) => ViewWithClick.fromViewAndClick(v, c) }
          .map(ViewWithClick.encodeCSV)

        viewsWithClicksCSV.outlet ~> fileSink("ViewsWithClicks.csv")

        val viewsEventsWindow =
          new Window[View, ViewableViewEvent, View.Id](_.id, _.interactionId)
        val viewableViews = viewsEventsMerge.out
          .mapConcat(viewsEventsWindow.push)
          .alsoTo(Sink.foreach(println(_)))
          .map(_._1)
          .map(ViewableView.apply)
        val viewableViewsBc = b.add(Broadcast[ViewableView](2))

        viewableViews.outlet ~> viewableViewsBc
        viewableViewsBc.outlet.map(ViewableView.encodeCSV) ~> fileSink("ViewableViews.csv")
        viewableViewsBc ~> Sink.ignore

        ClosedShape
    })

//    for {
//      _ <- viewsWithClicks
//        .map(ViewWithClick.encodeCSV)
//        .runWith(fileSink("ViewsWithClicks.csv"))
//      _ <- viewableViews
//        .map(ViewableView.encodeCSV)
//        .runWith(fileSink("ViewableViews.csv"))
//      _ <- ac.terminate()
//    } yield ()
    try {
      graph.run()

    } finally  {
      ac.terminate()

    }

  }

}
