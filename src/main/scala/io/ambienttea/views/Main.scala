package io.ambienttea.views

import java.time.Instant

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import com.typesafe.scalalogging.LazyLogging
import io.ambienttea.views.model._
import io.ambienttea.views.stream.DecodeCSV._
import io.ambienttea.views.stream.WindowedJoin
import io.ambienttea.views.stream.WindowedJoin.Window
import io.ambienttea.views.utils._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

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

    val graph =
      RunnableGraph.fromGraph[Future[IOResult]](GraphDSL.create(Stats.sink) {
        implicit b: GraphDSL.Builder[Future[IOResult]] => statsSink =>
          import GraphDSL.Implicits._

          val clicksBc = b.add(Broadcast[Click](2))
          val viewsBc = b.add(Broadcast[View](3))
          val viewableViewsBc = b.add(Broadcast[ViewableView](2))

          val mergeForStats = b.add(new Merge[ModelEvent](3, false))

          val viewsClicksJoin = b.add(
            WindowedJoin.shape[View, Click, Instant, View.Id](
              _.id,
              _.interactionId,
              _.logtime,
              _.logtime
            )
          )
          val viewsEventsJoin = b.add(
            WindowedJoin.shape[View, ViewableViewEvent, Instant, View.Id](
              _.id,
              _.interactionId,
              _.logtime,
              _.logtime
            )
          )

          clicksSource ~> clicksBc ~> viewsClicksJoin.in1
          viewsSource ~> viewsBc ~> viewsClicksJoin.in0
          viewsBc ~> viewsEventsJoin.in0
          viewableViewEventsSource ~> viewsEventsJoin.in1

          viewsClicksJoin.out
            .map { case (v, c) => ViewWithClick.fromViewAndClick(v, c) } ~>
            fileSink(ViewWithClick.encodeCSV, "ViewsWithClicks.csv")

          viewsEventsJoin.out.map { case (view, _) => ViewableView(view) } ~>
            viewableViewsBc

          viewableViewsBc ~>
            fileSink(ViewableView.encodeCSV, "ViewableViews.csv")

          clicksBc ~> mergeForStats.in(0)
          viewsBc ~> mergeForStats.in(1)
          viewableViewsBc ~> mergeForStats.in(2)

          mergeForStats ~> statsSink

          ClosedShape
      })

    try {
      graph.run().andThen { case _ => ac.terminate() }
    } catch {
      case _: Exception => ac.terminate()
    }

  }

}
