organization := "com.readr.spark"

name := "spark-index"

version := "1.0-SNAPSHOT"

// disable using the Scala version in output paths and artifacts
crossPaths := false

libraryDependencies ++= Seq( 
  "org.apache.spark" %% "spark-core" % "1.0.0" % "provided",
  "org.apache.hadoop" % "hadoop-client" % "1.0.4",
  "net.java.dev.jets3t"      % "jets3t"           % "0.7.1",
  "com.readr"  % "spark-base" % "1.0-SNAPSHOT",
  "com.readr"  % "model" % "1.0-SNAPSHOT",
  "org.apache.lucene" % "lucene-core" % "4.9.0",
  "org.apache.lucene" % "lucene-analyzers-common" % "4.9.0"
)

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers ++= Seq(
  "Readr snapshots" at "http://snapshots.mvn-repo.readr.com",
  "Readr releases" at "http://releases.mvn-repo.readr.com"
)

publishMavenStyle := false

publishTo := {
  if (isSnapshot.value)
    Some("snapshots" at "http://snapshots.mvn-repo.readr.com")
  else
    Some("releases" at "http://releases.mvn-repo.readr.com")
}

EclipseKeys.withSource := true


