organization := "com.readr.spark"

name := "malt"

version := "1.2-SNAPSHOT"

scalaVersion := "2.11.4"

libraryDependencies ++= Seq( 
  "org.apache.spark" % "spark-core_2.10" % "1.1.1" % "provided",
  "org.maltparser" % "maltparser"  % "1.8",  
  "com.readr.spark" %% "base" % "1.2-SNAPSHOT",
  "com.readr" %% "model" % "1.2-SNAPSHOT"
)

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

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
