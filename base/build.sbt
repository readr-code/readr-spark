organization := "com.readr.spark"

name := "base"

version := "1.2-SNAPSHOT"

scalaVersion := "2.11.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.3.0-SNAPSHOT" % "provided",
  "org.apache.hadoop" % "hadoop-client" % "1.0.4",
  "commons-configuration" % "commons-configuration" % "1.9"
 )

libraryDependencies += "org.scalatest" %% "scalatest" % "2.1.6" % "test"

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
