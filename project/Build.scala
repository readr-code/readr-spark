import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

object ReadrSparkBuild extends Build {
  lazy val buildSettings = Defaults.defaultSettings ++ Seq(
    version := "1.0-SNAPSHOT",
    organization := "com.readr.spark"
  )

  lazy val root = Project(id = "root", base = file(".")) aggregate(
    subBase,
    subIndex,
    subMain,
    subCj,
    subMalt,
    subStanford34,
//    subAllenai,
    subFrame,
    subOther)

  def subproject(name: String) = Project(id = name, base = file(name))

  lazy val subBase = subproject("base")
  lazy val subIndex = subproject("index") dependsOn(subBase)
  lazy val subCj = subproject("cj") dependsOn(subBase)
  lazy val subMalt = subproject("malt") dependsOn(subBase)
  lazy val subStanford34 = subproject("stanford34") dependsOn(subBase)
//  lazy val subAllenai = subproject("allenai") dependsOn(subBase)
  lazy val subFrame = subproject("frame") dependsOn(subBase)
  lazy val subOther = subproject("other") dependsOn(subBase)
  lazy val subMain = Project(id = "main", base = file("main"), settings = buildSettings ++ assemblySettings) dependsOn(subBase,subIndex,subCj,subMalt,subStanford34,subFrame,subOther)
//  lazy val subMain = Project(id = "main", base = file("main"), settings = buildSettings ++ assemblySettings) dependsOn(subBase,subIndex,subCj,subMalt,subStanford34,subAllenai,subFrame,subOther)
}
