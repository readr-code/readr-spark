organization := "com.readr.spark"

name := "index"

version := "1.2-SNAPSHOT"

scalaVersion := "2.11.4"

libraryDependencies ++= Seq( 
  "org.apache.spark" %% "spark-core" % "1.3.0-SNAPSHOT" % "provided",
  "com.readr.spark"  %% "base" % "1.2-SNAPSHOT",
  "com.readr" %% "model" % "1.2-SNAPSHOT",
  "org.apache.lucene" % "lucene-core" % "4.9.0",
  "org.apache.lucene" % "lucene-analyzers-common" % "4.9.0"
)

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += Resolver.mavenLocal

resolvers ++= Seq(
  "Readr snapshots" at "http://snapshots.mvn-repo.readr.com",
  "Readr releases" at "http://releases.mvn-repo.readr.com"
)

publishMavenStyle := true

publishTo := {
  if (isSnapshot.value)
    Some("snapshots" at "s3://snapshots.mvn-repo.readr.com")
  else
    Some("releases" at "s3://releases.mvn-repo.readr.com")
}
