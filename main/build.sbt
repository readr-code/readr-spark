organization := "com.readr.spark"

name := "main"

version := "1.2-SNAPSHOT"

scalaVersion := "2.11.4"

// NOTE: For hadoop 2.4.0 must also include different version of jets3t
// "org.apache.hadoop" % "hadoop-client" % "2.4.0",
// "net.java.dev.jets3t"      % "jets3t"           % "0.9.0",
//  "org.apache.hadoop" % "hadoop-client" % "1.0.4",
//  "net.java.dev.jets3t"      % "jets3t"           % "0.7.1",
// but default version on spark ec2 is 1.0.4

libraryDependencies ++= Seq( 
  "org.apache.spark" %% "spark-core" % "1.3.0-SNAPSHOT" % "provided",
//  "ooyala.cnd" % "job-server" % "0.3.1" % "provided",
  "com.twitter" %% "chill" % "0.5.1",
  "com.readr.spark"  %% "base" % "1.2-SNAPSHOT",
  "com.readr"  %% "model" % "1.2-SNAPSHOT",
////  "com.readr"  %% "client" % "1.1-SNAPSHOT",
//  "com.readr.spark" %% "cj" % "1.1-SNAPSHOT",
  "com.readr.spark" %% "index" % "1.2-SNAPSHOT",
//  "com.readr.spark" %% "malt" % "1.1-SNAPSHOT",
////  "com.readr.spark" %% "allenai" % "1.1-SNAPSHOT",
  "com.readr.spark" %% "stanford34" % "1.2-SNAPSHOT",
  "com.readr.spark" %% "other" % "1.2-SNAPSHOT",
  "com.readr.spark" %% "frame" % "1.2-SNAPSHOT"  
)

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += Resolver.mavenLocal

//resolvers += "Ooyala Bintray" at "http://dl.bintray.com/ooyala/maven"

resolvers ++= Seq(
  "Readr snapshots" at "http://snapshots.mvn-repo.readr.com",
  "Readr releases" at "http://releases.mvn-repo.readr.com"
)

resolvers ++= Seq(
  "AllenAI Snapshots" at "http://utility.allenai.org:8081/nexus/content/repositories/snapshots",
  "AllenAI Releases" at "http://utility.allenai.org:8081/nexus/content/repositories/releases",
  "IESL Releases" at "http://dev-iesl.cs.umass.edu/nexus/content/groups/public",
  Resolver.sonatypeRepo("snapshots")
)

publishMavenStyle := true

publishTo := {
  if (isSnapshot.value)
    Some("snapshots" at "s3://snapshots.mvn-repo.readr.com")
  else
    Some("releases" at "s3://releases.mvn-repo.readr.com")
}

