organization := "com.readr.spark"

name := "spark-stanford34"

version := "1.0-SNAPSHOT"

libraryDependencies ++= Seq( 
  "org.apache.spark" %% "spark-core" % "1.0.2" % "provided",
  "com.readr"  %% "spark-base" % "1.0-SNAPSHOT",
  "com.readr"  % "model" % "1.0-SNAPSHOT",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.4" withSources(),
  "edu.stanford.nlp" % "stanford-corenlp" % "3.4" classifier "models"
)

//unmanagedJars in Compile += file("lib/stanford-corenlp-3.4.jar")

//unmanagedJars in Compile += file("lib/stanford-srparser-2014-06-16-models.jar")

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers ++= Seq(
  "Readr snapshots" at "http://snapshots.mvn-repo.readr.com",
  "Readr releases" at "http://releases.mvn-repo.readr.com"
)

publishMavenStyle := false

publishTo := {
  if (isSnapshot.value)
    Some("snapshots" at "s3://snapshots.mvn-repo.readr.com")
  else
    Some("releases" at "s3://releases.mvn-repo.readr.com")
}

EclipseKeys.withSource := true


