organization := "org.bitbonanza"

name := "Akka X Scheduling"

scalaVersion := "2.10.2"

resolvers += "Sonatype Nexus Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/snapshots/"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.3-SNAPSHOT"

//libraryDependencies += "com.typesafe.akka" %% "akka-remote" % "2.3-SNAPSHOT"

libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % "2.3-SNAPSHOT" % "test"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.5" // MIT

libraryDependencies += "org.scalatest" %% "scalatest" % "2.0" % "test"

libraryDependencies += "org.mapdb" % "mapdb" % "0.9.9-SNAPSHOT" // Apache 2.0

// Uses forked version in lib directory instead (git://github.com/odd/prequel.git)
//libraryDependencies += "net.noerd" %% "prequel" % "0.3.8" // wtfpl

// Start of dependencies copied from prequel
libraryDependencies += "commons-pool" % "commons-pool" % "1.5.5"

libraryDependencies += "commons-dbcp" % "commons-dbcp" % "1.4"

libraryDependencies += "commons-lang" % "commons-lang" % "2.6"

libraryDependencies += "joda-time" % "joda-time" % "2.1"

libraryDependencies += "org.joda" % "joda-convert" % "1.2"
// End of dependencies copied from prequel

libraryDependencies += "org.hsqldb" % "hsqldb" % "2.3.1" % "test"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.28" % "test"

//libraryDependencies += "com.github.philcali" %% "cronish" % "0.1.3" // MIT

