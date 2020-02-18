package io.ambienttea.views.stream

import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Failure, Success, Try}

object DecodeCSV {
  implicit class decodeOps[E](source: Source[String, E]) extends LazyLogging {
    def decodeCSV[R](decoder: String => Try[R]): Source[R, E] =
      source
        .drop(1) // header
        .mapConcat(input =>
          decoder(input) match {
            case Success(value) => List(value)
            case Failure(exception) =>
              logger
                .error(s"Failed decoding csv '$input': $exception", exception)
              Nil
          }
        )
  }
}
