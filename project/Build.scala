import sbt._
import Keys._

object SparkReadrBuild extends Build {
  lazy val root = Project(id = "root", base = file(".")) aggregate(
    sparkBase,
    sparkIndex,
    sparkReadr,
    sparkCj,
    sparkMalt,
    sparkStanford34,
    sparkAllenai,
    sparkFrame,
    sparkOther)

  def subproject(name: String) = Project(id = name, base = file(name))

  lazy val sparkBase = subproject("spark-base")
  lazy val sparkIndex = subproject("spark-index") dependsOn(sparkBase)
  lazy val sparkReadr = subproject("spark-readr") dependsOn(sparkBase)
  lazy val sparkCj = subproject("spark-cj") dependsOn(sparkBase)
  lazy val sparkMalt = subproject("spark-malt") dependsOn(sparkBase)
  lazy val sparkStanford34 = subproject("spark-stanford34") dependsOn(sparkBase)
  lazy val sparkAllenai = subproject("spark-allenai") dependsOn(sparkBase)
  lazy val sparkFrame = subproject("spark-frame") dependsOn(sparkBase)
  lazy val sparkOther = subproject("spark-other") dependsOn(sparkBase)
}
