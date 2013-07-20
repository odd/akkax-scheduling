organization := "org.bitbonanza"

name := "Akka X Scheduling"

scalaVersion := "2.10.2"

resolvers += "Sonatype Nexus Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.2.0"

libraryDependencies += "com.typesafe.akka" %% "akka-remote" % "2.2.0"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.5" // MIT

libraryDependencies += "org.scalatest" %% "scalatest" % "1.9.1" % "test"

libraryDependencies += "org.mapdb" % "mapdb" % "0.9.4-SNAPSHOT" // Apache 2.0

//libraryDependencies += "com.github.philcali" %% "cronish" % "0.1.3" // MIT

