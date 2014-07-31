organization := "com.readr"

name := "spark-base"

version := "1.0-SNAPSHOT"

// disable using the Scala version in output paths and artifacts
crossPaths := false

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.0.0" % "provided",
  "org.apache.hadoop" % "hadoop-client" % "1.0.4",
  "commons-configuration" % "commons-configuration" % "1.9"
 ) 

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

EclipseKeys.withSource := true

