scalaVersion := "2.13.0"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"

libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.6.3"
resolvers += Resolver.bintrayRepo("zamblauskas", "maven")

libraryDependencies += "zamblauskas" %% "scala-csv-parser" % "0.11.6"
