package io.ambienttea.views

import com.typesafe.scalalogging.LazyLogging
import akka.stream.scaladsl.{FileIO, Framing, Source}
import java.nio.file.{Path, Paths}
import java.net.URI

import akka.actor.ActorSystem
import akka.util.ByteString
import io.ambienttea.views.model.{Click, View, ViewableView}
import utils._

object Main extends LazyLogging {
  def main(args: Array[String]) {
    logger.info(s"Server started with arguments: $args")
    val viewsFileName = args(0)
    val clicksFileName = args(1)
    val viewableViewsFileName = args(2)

    implicit val ac: ActorSystem = ActorSystem()

    val viewsSource = fileSource(viewsFileName)
    val clicksSource = fileSource(clicksFileName)
    val viewableViewsSource = fileSource(viewableViewsFileName)

    viewsSource
      .drop(1)
      .take(1)
      .map(View.decode)
      .mapConcat(_.toOption.toSeq)
      .runForeach(s => println(s">>> $s"))
    clicksSource
      .drop(1)
      .take(1)
      .map(Click.decode)
      .mapConcat(_.toOption.toSeq)
      .runForeach(s => println(s">>> $s"))
    viewableViewsSource
      .drop(1)
      .take(1)
      .map(ViewableView.decode)
      .mapConcat(_.toOption.toSeq)
      .runForeach(s => println(s">>> $s"))



    ac.terminate()
  }

}
