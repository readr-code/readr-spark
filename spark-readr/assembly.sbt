import AssemblyKeys._ // put this at the top of the file

assemblySettings

// your assembly settings here

mainClass in assembly := Some("com.readr.spark.stanfordToken.App")

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("play", xs @ _*)         => MergeStrategy.first
    case PathList("org", "apache", "commons", xs @ _*)  => MergeStrategy.first
    case PathList("org", "w3c", xs @ _*)  => MergeStrategy.first
    case PathList("org", "allenai", xs @ _*)  => MergeStrategy.last
    case PathList("com", "esotericsoftware", x @ _*) => MergeStrategy.first
    case PathList("java_cup", "runtime", xs @ _*) => MergeStrategy.last
    case x => old(x)
  }
}

// Note: we must exclude hadoop-core-1.0.4.jar, as it causes
// a "IncompatibleClassChangeError: Implementing class";
// Issue seems to be with org.apache.hadoop.mapred.JobContextImpl
// which does not exist in this version of hadoop, and is
// referenced in SparkHadoopMapRedUtil.scala

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
   "asm-3.2.jar",
//    "stanford-corenlp-3.2.0-models.jar",
//    "stanford-corenlp-3.2.0.jar",
    "commons-beanutils-1.7.0.jar",
    "commons-beanutils-core-1.8.0.jar",
    "hadoop-core-1.0.4.jar"
//    "jcl-over-slf4j-1.7.6.jar",
//    "xml-apis-1.4.01.jar",
//    "akka-actor_2.10-2.3.3.jar",
//    "akka-slf4j_2.10-2.3.3.jar",
//    "jcl-over-slf4j-1.7.6.jar",
//    "slf4j-log4j12-1.7.2.jar",
//    "xom-1.2.5.jar",
//    "play_2.10-2.3.0-RC2.jar",
//    "build-link-2.3.0-RC2.jar"
)
  cp filter {x => ar.contains(x.data.getName)}
}
