organization := "org.bitbonanza"

name := "Akka X Scheduling"

scalaVersion := "2.10.1"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.2-M2"

libraryDependencies += "com.typesafe.akka" %% "akka-remote" % "2.2-M2"

//libraryDependencies += "com.github.philcali" %% "cronish" % "0.1.3" // MIT

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.2" // MIT

libraryDependencies += "org.scalatest" %% "scalatest" % "1.9.1" % "test"
