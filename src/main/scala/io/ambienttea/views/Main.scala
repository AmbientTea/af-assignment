package io.ambienttea.views

import com.typesafe.scalalogging.LazyLogging
import akka.stream.scaladsl.{FileIO, Framing, Source}
import java.nio.file.{Path, Paths}
import java.net.URI

import akka.actor.ActorSystem
import akka.util.ByteString
import io.ambienttea.views.model.{Click, View, ViewableView}
import io.ambienttea.views.stream.WindowedMerge
import utils._
import stream.DecodeCSV._

import scala.concurrent.ExecutionContext
import ExecutionContext.Implicits.global

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

    val viewableViewsSource = fileSource(viewableViewsFileName)
      .decodeCSV(ViewableView.decode)

//    val viewsWithClicksStream =
//      WindowedMerge(viewsSource, clicksSource)(_.logtime, _.logtime)
//
//    for {
//      _ <- viewsWithClicksStream.runForeach(s => println(s">>> $s"))
////      _ <- viewsSource.runForeach(s => println(s">>> $s"))
////      _ <- clicksSource.runForeach(s => println(s">>> $s"))
//      _ <- ac.terminate()
//    } yield ()
  }

}
