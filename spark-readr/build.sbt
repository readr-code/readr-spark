organization := "com.readr.spark"

name := "spark-readr"

version := "1.0-SNAPSHOT"

// disable using the Scala version in output paths and artifacts
crossPaths := false

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.0.0" % "provided"

// NOTE: For hadoop 2.4.0 must also include different version of jets3t
// "org.apache.hadoop" % "hadoop-client" % "2.4.0",
// "net.java.dev.jets3t"      % "jets3t"           % "0.9.0",

libraryDependencies ++= Seq( 
  "org.apache.spark" %% "spark-core" % "1.0.0" % "provided",
  "org.apache.hadoop" % "hadoop-client" % "1.0.4",
  "net.java.dev.jets3t"      % "jets3t"           % "0.7.1",
  "com.twitter"  %% "chill" % "0.3.6",
  "com.readr"  % "spark-base" % "1.0-SNAPSHOT",
  "com.readr"  % "model" % "1.0-SNAPSHOT",
//  "com.readr"  % "client" % "1.0-SNAPSHOT",
  "com.readr.spark" % "spark-cj" % "1.0-SNAPSHOT",
  "com.readr.spark" % "spark-index" % "1.0-SNAPSHOT",
  "com.readr.spark" % "spark-malt" % "1.0-SNAPSHOT",
  "com.readr.spark" % "spark-allenai" % "1.0-SNAPSHOT",
  "com.readr.spark" % "spark-stanford34" % "1.0-SNAPSHOT",
  "com.readr.spark" % "spark-other" % "1.0-SNAPSHOT",
  "com.readr.spark" % "spark-frame" % "1.0-SNAPSHOT"  
//  "com.readr" % "import-allenai-barrons-spark" % "1.0-SNAPSHOT"
//  "org.scala-lang" % "scala-library" % "2.10.4" % "provided",
//  "com.typesafe.play" % "play_2.10" % "2.3.0-RC2" % "provided"
)

//unmanagedJars in Compile += file("../spark-stanford34/lib/stanford-corenlp-3.4.jar")

//unmanagedJars in Compile += file("../spark-stanford34/lib/stanford-corenlp-3.4-models.jar")

//unmanagedJars in Compile += file("../spark-stanford34/lib/stanford-srparser-2014-06-16-models.jar")

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers ++= Seq(
  "Readr snapshots" at "s3://snapshots.mvn-repo.readr.com",
  "Readr releases" at "s3://releases.mvn-repo.readr.com"
)

publishMavenStyle := false

publishTo := {
  if (isSnapshot.value)
    Some("snapshots" at "s3://snapshots.mvn-repo.readr.com")
  else
    Some("releases" at "s3://releases.mvn-repo.readr.com")
}

resolvers ++= Seq(
  "AllenAI Snapshots" at "http://utility.allenai.org:8081/nexus/content/repositories/snapshots",
  "AllenAI Releases" at "http://utility.allenai.org:8081/nexus/content/repositories/releases",
  "IESL Releases" at "http://dev-iesl.cs.umass.edu/nexus/content/groups/public",
  Resolver.sonatypeRepo("snapshots")
)

// but default version on spark ec2 is 1.0.4

EclipseKeys.withSource := true

net.virtualvoid.sbt.graph.Plugin.graphSettings
