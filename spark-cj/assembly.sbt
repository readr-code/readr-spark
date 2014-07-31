import AssemblyKeys._ // put this at the top of the file

assemblySettings

// your assembly settings here

mainClass in assembly := Some("com.readr.spark.cj.App")

excludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  val ar = Array[String]("servlet-api-2.5-20081211.jar", 
    "servlet-api-2.5-6.1.14.jar",
//    "javax.servlet-2.5.0.v201103041518.jar",
    "hadoop-yarn-api-2.4.0.jar",
    "jasper-runtime-5.5.12.jar",
    "jasper-compiler-5.5.12.jar",
    "xml-apis-1.3.03.jar",
    "minlog-1.2.jar",
    "hadoop-yarn-api-2.2.0.jar",
    "asm-3.2.jar"
//    "stanford-corenlp-3.2.0-models.jar",
//    "stanford-corenlp-3.2.0.jar"
)
  cp filter {x => ar.contains(x.data.getName)}
}
