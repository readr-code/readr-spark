//package com.readr.spark.cli
//
//import java.io.File
//import java.io.IOException
//import java.net.URL
//import java.net.URLDecoder
//import java.util.Arrays
//import java.util.Enumeration
//import org.apache.commons.configuration.PropertiesConfiguration
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs.FileStatus
//import org.apache.hadoop.fs.FileSystem
//import org.apache.hadoop.fs.Path
//import com.readr.common.ConfigurationManager
//import com.readr.spark.common.HadoopConfigurationUtils
//import com.readr.spark.common.ProcessModule
//
//object CLI {
//
//  def usage() = {
//    println("""Usage: run [--jar <jar>] COMMAND [moduleOptions]
//where COMMAND is one of:
//   local            Process locally
//   hadoop           Process on hadoop cluster
//   emr-run          Process on ElasticMapReduce
//   emr-copy         Copy to S3 for ElasticMapReduce
//   info             Show information about this module""")
//  }
//
//  def run(moduleClazz:Class[_ <: ProcessModule], args:Array[String]):Unit = {
//
//    if (args.length < 1) {
//	  usage
//	  return
//	}
//
//	val command = args(0)
//	val remArgs = args.slice(1, args.length)
//
//	command match {
//	  case "local" =>
//	    local(moduleClazz, remArgs)
//	  case "hadoop" =>
//	    hadoop(moduleClazz, remArgs)
//	  case "emr-copy" =>
//	    emrcopy(moduleClazz, remArgs)
//	  case "emr-run" =>
//	    emrrun(moduleClazz, remArgs)
//	  case "info" =>
//	    info(moduleClazz, remArgs)
//	}
//  }
//
//  def local(moduleClazz:Class[_ <: ProcessModule], args:Array[String]) = {
//    val jarFile = findContainingJar(moduleClazz)
//	// run locally
//	// set up hadoop configuration object
//	val configuration = new Configuration()
//	// set execution to local
//	configuration.set("mapred.job.tracker", "local")
//	configuration.set("dfs.replication", "1")
//	configuration.set("fs.default.name", "file:///")
//	configuration.set("job.local.dir", "/tmp")
//
//	CLIrun.run(jarFile, true, configuration, moduleClazz, args);
//  }
//
//  def hadoop(moduleClazz:Class[_ <: ProcessModule], args:Array[String]) = {
//	val jarFile = findContainingJar(moduleClazz)
//
//	// reads HADOOP_HOME from readr/conf, then reads all hadoop config files
//	val configuration = HadoopConfigurationUtils.getConfiguration()
//	CLIrun.run(jarFile, false, configuration, moduleClazz, args)
//  }
//
//  def emrrun(moduleClazz:Class[_ <: ProcessModule], args:Array[String]) = {
//	// run on hadoop, use default hadoop configuration
//	//Configuration configuration = HadoopConfigurationUtils.getConfiguration();
//	val jarFile = findContainingJar(moduleClazz)
//	val configuration = new Configuration()
//	CLIrun.run(jarFile, false, configuration, moduleClazz, args)
//  }
//  
//  def emrcopy(moduleClazz:Class[_ <: ProcessModule], args:Array[String]) = {
//
//	// 1. find containing jar
//	val jarFile = findContainingJar(moduleClazz)
//	
//	// 2. copy to S3
//	val configuration = ConfigurationManager.get().getConfiguration()
//	val prefix = "s3n://" + configuration.getString("AWS_s3bucket") + "/code/"
//	
//	val hadConf = new Configuration()
//	hadConf.set("java.security.krb5.realm", "")
//	hadConf.set("java.security.krb5.kdc", "")
//	hadConf.set("fs.default.name", "s3n://" + configuration.getString("AWS_s3bucket"))
//	hadConf.set("fs.s3n.awsAccessKeyId", configuration.getString("AWS_accessKey"))
//	hadConf.set("fs.s3n.awsSecretAccessKey", configuration.getString("AWS_secretKey"))
//
//	val dfs = FileSystem.get(hadConf)
//	val lfs = FileSystem.getLocal(hadConf)
//	val p = new Path(prefix)
//	if (!dfs.exists(p)) {
//		dfs.mkdirs(p)
//	}
//	val f = new File(jarFile)
//	val remoteFile = prefix + f.getName()
//	val pathLocal = new org.apache.hadoop.fs.Path(jarFile)
//	val pathHDFS = new org.apache.hadoop.fs.Path(remoteFile)
//	if (dfs.exists(pathHDFS)) {
//	  val remoteFs = dfs.getFileStatus(pathHDFS);
//	  val localFs = lfs.getFileStatus(pathLocal);
//		
//	  // check if lengths match and modification time is newer
//	  // on s3, then we assume they are identical and skip copying			
//	  if (remoteFs.getLen() == localFs.getLen() &&
//			remoteFs.getModificationTime() >= localFs.getModificationTime()) {
//		println("Newer file of same size already exists on S3, skipping copying")
//	  } else {
//		dfs.delete(pathHDFS, true)
//		println("Copying " + jarFile + " to " + prefix)
//		dfs.copyFromLocalFile(pathLocal, pathHDFS)
//	  }
//	} else {
//	  println("Copying " + jarFile + " to " + prefix)
//	  dfs.copyFromLocalFile(pathLocal, pathHDFS)
//	}
//	
//	// 3. submit job flow on EMR
//	
//	//    args: hadoop ...
//	//    this will run this with hadoop mode on EMR
//  }
//
//  def info(moduleClazz:Class[_ <: ProcessModule], args:Array[String]) = {
//	val module = moduleClazz.newInstance()
//	module.printInfo()
//  }
//
//	// copied from hadoop JobConf
//  def findContainingJar(my_class:Class[_]):String = {
//	val loader = my_class.getClassLoader()
//	val class_file = my_class.getName().replaceAll("\\.", "/") + ".class"
//	try {
//	  val itr:Enumeration[URL] = loader.getResources(class_file)
//	  while (itr.hasMoreElements) {
//		val url = itr.nextElement().asInstanceOf[URL]
//		if ("jar".equals(url.getProtocol())) {
//		  var toReturn = url.getPath()
//		  if (toReturn.startsWith("file:")) {
//			toReturn = toReturn.substring("file:".length())
//		  }
//		  toReturn = URLDecoder.decode(toReturn, "UTF-8")
//		  return toReturn.replaceAll("!.*$", "")
//		}
//	  }
//	} catch {
//	  case ioe:java.io.IOException =>
//	    throw new RuntimeException(ioe)
//	}
//	return null
//  }
//}
