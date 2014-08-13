organization := "com.readr.spark"

name := "spark-allenai"

version := "1.0-SNAPSHOT"

libraryDependencies ++= Seq( 
  "org.apache.spark" %% "spark-core" % "1.0.2" % "provided",
  "com.readr"  % "spark-base" % "1.0-SNAPSHOT",
  "com.readr"  % "model" % "1.0-SNAPSHOT"
)

libraryDependencies ++= Seq(
  "org.allenai.nlpstack" %% "nlpstack-core" % "0.6",
  "org.allenai.nlpstack" %% "nlpstack-parse" % "0.6",
  "org.allenai.nlpstack" %% "nlpstack-postag" % "0.6",
  "org.allenai.nlpstack" %% "nlpstack-segment" % "0.6",
  "org.allenai.nlpstack" %% "nlpstack-lemmatize" % "0.6"
)

resolvers ++= Seq(
  "AllenAI Snapshots" at "http://utility.allenai.org:8081/nexus/content/repositories/snapshots",
  "AllenAI Releases" at "http://utility.allenai.org:8081/nexus/content/repositories/releases",
  "IESL Releases" at "http://dev-iesl.cs.umass.edu/nexus/content/groups/public",
  Resolver.sonatypeRepo("snapshots")
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
