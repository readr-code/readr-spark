organization := "com.readr.spark"

name := "netlib"

version := "1.1-SNAPSHOT"

libraryDependencies ++= Seq( 
  "com.github.fommil.netlib" % "all" % "1.1.2"
)

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers ++= Seq(
  "Readr snapshots" at "s3://snapshots.mvn-repo.readr.com",
  "Readr releases" at "s3://releases.mvn-repo.readr.com"
)

publishMavenStyle := true

publishTo := {
  if (isSnapshot.value)
    Some("snapshots" at "s3://snapshots.mvn-repo.readr.com")
  else
    Some("releases" at "s3://releases.mvn-repo.readr.com")
}
