package io.ambienttea.views

import java.nio.file.Paths

import akka.stream.IOResult
import akka.stream.scaladsl.{FileIO, Flow, Framing, Sink, Source}
import akka.util.ByteString

import scala.concurrent.Future

package object utils {
  def fileSource(filename: String): Source[String, Future[IOResult]] = {

    FileIO
      .fromPath(Paths.get(filename))
      .via(Framing.delimiter(ByteString("\n"), 1024, allowTruncation = true))
      .map(_.utf8String)
  }

  def fileSink(filename: String): Sink[String, Future[IOResult]] = {
    FileIO
      .toPath(Paths.get(filename))
      .contramap[String](ByteString.fromString)
      .contramap(_ + "\n")
  }

  def fileSink[T](
      encode: T => String,
      filename: String
  ): Sink[T, Future[IOResult]] =
    fileSink(filename).contramap(encode)
}
