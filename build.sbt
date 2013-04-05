organization := "org.bitbonanza"

name := "Akka X Scheduling"

scalaVersion := "2.10.1"

resolvers += "Sonatype Nexus Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.2-M2"

libraryDependencies += "com.typesafe.akka" %% "akka-remote" % "2.2-M2"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.2" // MIT

libraryDependencies += "org.scalatest" %% "scalatest" % "1.9.1" % "test"

libraryDependencies += "org.mapdb" % "mapdb" % "0.9-SNAPSHOT" // Apache 2.0

// Uses forked version in lib directory instead (git://github.com/odd/prequel.git)
//libraryDependencies += "net.noerd" %% "prequel" % "0.3.8" // wtfpl

// Start of dependencies copied from prequel
libraryDependencies += "commons-pool" % "commons-pool" % "1.5.5"

libraryDependencies += "commons-dbcp" % "commons-dbcp" % "1.4"

libraryDependencies += "commons-lang" % "commons-lang" % "2.6"

libraryDependencies += "joda-time" % "joda-time" % "2.1"

libraryDependencies += "org.joda" % "joda-convert" % "1.2"
// End of dependencies copied from prequel

libraryDependencies += "org.hsqldb" % "hsqldb" % "2.2.4" % "test"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.24" % "test"

//libraryDependencies += "com.github.philcali" %% "cronish" % "0.1.3" // MIT

