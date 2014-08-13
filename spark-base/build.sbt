organization := "com.readr"

name := "spark-base"

version := "1.0-SNAPSHOT"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.0.2" % "provided",
  "org.apache.hadoop" % "hadoop-client" % "1.0.4",
  "commons-configuration" % "commons-configuration" % "1.9"
 ) 

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

