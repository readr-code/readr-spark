import sbt._
import sbt.Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

object ReadrSparkBuild extends Build {
  lazy val buildSettings = Defaults.defaultSettings ++ Seq(
    version := "1.1-SNAPSHOT",
    organization := "com.readr.spark"
  )

  lazy val root = Project(id = "readr-spark", base = file("."), settings = buildSettings ++ assemblySettings ++ Seq(
    publishTo := {
      if (isSnapshot.value)
        Some("snapshots" at "s3://snapshots.mvn-repo.readr.com")
      else
        Some("releases" at "s3://releases.mvn-repo.readr.com")
    },
    resolvers ++= Seq[Resolver](
      "Readr snapshots" at "http://snapshots.mvn-repo.readr.com",
      "Readr releases" at "http://releases.mvn-repo.readr.com"
    ),
    publishMavenStyle := true
  )) aggregate(
    subBase,
    subIndex,
    subMain,
    subCj,
    subMalt,
    subStanford34,
    subAllenai,
    subFrame,
    subOther)

  def subproject(name: String) = Project(id = name, base = file(name))

  lazy val subBase = subproject("base")
  lazy val subIndex = subproject("index") dependsOn(subBase)
  lazy val subCj = subproject("cj") dependsOn(subBase)
  lazy val subMalt = subproject("malt") dependsOn(subBase)
  lazy val subStanford34 = subproject("stanford34") dependsOn(subBase)
  lazy val subAllenai = subproject("allenai") dependsOn(subBase)
  lazy val subFrame = subproject("frame") dependsOn(subBase,subIndex)
  lazy val subOther = subproject("other") dependsOn(subBase)
//  lazy val subMain = Project(id = "main", base = file("main"), settings = buildSettings ++ assemblySettings) dependsOn(subBase,subIndex,subCj,subMalt,subStanford34,subFrame,subOther)
  lazy val subMain = Project(id = "main", base = file("main"), settings = buildSettings ++ assemblySettings) dependsOn(subBase,subIndex,subCj,subMalt,subStanford34,subAllenai,subFrame,subOther)
}
