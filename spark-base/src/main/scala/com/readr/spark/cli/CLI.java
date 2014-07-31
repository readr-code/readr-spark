//package com.readr.spark.cli;
//
//import java.io.File;
//import java.io.IOException;
//import java.net.URL;
//import java.net.URLDecoder;
//import java.util.Arrays;
//import java.util.Enumeration;
//
//import org.apache.commons.configuration.PropertiesConfiguration;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//
//import com.readr.common.ConfigurationManager;
//import com.readr.hadoop.common.HadoopConfigurationUtils;
//import com.readr.hadoop.module.ProcessModule;
//
//public class CLI {
//
//	static boolean DEBUG = false;
//	
//	private static void usage() {
//		System.out.println("Usage: process [--jar <jar>] COMMAND [moduleOptions]");
//		System.out.println("where COMMAND is one of:");
//		System.out.println("   local            Process locally");
//		System.out.println("   hadoop           Process on hadoop cluster");
//		System.out.println("   emr-run          Process on ElasticMapReduce");
//		System.out.println("   emr-copy         Copy to S3 for ElasticMapReduce");
//		System.out.println("   info             Show information about this module");
//	}
//
//	public static void run(Class<? extends ProcessModule> moduleClazz,
//			String[] args) {
//		// we would like to control the info/debug messages that are printed
//		// by the hadoop API; for this we add our custom log4j.properties
////		PropertyConfigurator.configure(CLI.class.getClassLoader().getResourceAsStream("log4j.properties"));
//		
//		// // check for --jar option
//		// String jar = null;
//		// OptionsParser parser = new OptionsParser();
//		// parser.parse(args);
//		// if (parser.hasOption("--jar"))
//		// jar = parser.getOptionValue("--jar");
//		// args = parser.getRemainingArgs();
//
//		if (args.length < 1) {
//			usage();
//			return;
//		}
//
//		String command = args[0];
//		String[] remArgs = Arrays.copyOfRange(args, 1, args.length);
//
//		try {
//			if (command.equals("local")) {
//				local(moduleClazz, remArgs);
//				return;
//			}
//			if (command.equals("hadoop")) {
//				hadoop(moduleClazz, remArgs);
//				return;
//			}
//			if (command.equals("emr-copy")) {
//				emrcopy(moduleClazz, remArgs);
//				return;
//			}
//			if (command.equals("emr-run")) {
//				emrrun(moduleClazz, remArgs);
//				return;
//			}
//			if (command.equals("info")) {
//				info(moduleClazz, remArgs);
//				return;
//			}
//		} catch (Throwable e) {
//			System.out.println(e.getMessage());
//			if (DEBUG) e.printStackTrace();
//		}
//	}
//
//	private static void local(Class<? extends ProcessModule> moduleClazz,
//			String[] args) throws Exception {
//		String jarFile = findContainingJar(moduleClazz);
//		// run locally
//		// set up hadoop configuration object
//		Configuration configuration = new Configuration();
//		// set execution to local
//		configuration.set("mapred.job.tracker", "local");
//		configuration.set("dfs.replication", "1");
//		configuration.set("fs.default.name", "file:///");
//		configuration.set("job.local.dir", "/tmp");
//
//		CLIrun.run(jarFile, true, configuration, moduleClazz, args);
//	}
//
//	private static void hadoop(Class<? extends ProcessModule> moduleClazz,
//			String[] args) throws Exception, Throwable {
//		String jarFile = findContainingJar(moduleClazz);
//
//		// reads HADOOP_HOME from readr/conf, then reads all hadoop config files
//		Configuration configuration = HadoopConfigurationUtils.getConfiguration();
//		CLIrun.run(jarFile, false, configuration, moduleClazz, args);
//	}
//
//	private static void emrrun(Class<? extends ProcessModule> moduleClazz,
//			String[] args) throws Exception, Throwable {
//		// run on hadoop, use default hadoop configuration
//		//Configuration configuration = HadoopConfigurationUtils.getConfiguration();
//		String jarFile = findContainingJar(moduleClazz);
//		Configuration configuration = new Configuration();
//		CLIrun.run(jarFile, false, configuration, moduleClazz, args);
//	}
//
//	private static void emrcopy(Class<? extends ProcessModule> moduleClazz, String[] args) throws Exception {
//
//		// 1. find containing jar
//		String jarFile = findContainingJar(moduleClazz);
//		
//		// 2. copy to S3
//		PropertiesConfiguration configuration = ConfigurationManager.get().getConfiguration();
//		String prefix = "s3n://" + configuration.getString("AWS_s3bucket") + "/code/";
//		
//		Configuration hadConf = new Configuration();
//		hadConf.set("java.security.krb5.realm", "");
//		hadConf.set("java.security.krb5.kdc", "");
//		hadConf.set("fs.default.name", "s3n://" + configuration.getString("AWS_s3bucket"));
//		hadConf.set("fs.s3n.awsAccessKeyId", configuration.getString("AWS_accessKey"));
//		hadConf.set("fs.s3n.awsSecretAccessKey", configuration.getString("AWS_secretKey"));
//
//		FileSystem dfs = FileSystem.get(hadConf);
//		FileSystem lfs = FileSystem.getLocal(hadConf);
//		Path p = new Path(prefix);
//		if (!dfs.exists(p)) {
//			dfs.mkdirs(p);
//		}
//		File f = new File(jarFile);
//		String remoteFile = prefix + f.getName();
//		org.apache.hadoop.fs.Path pathLocal = new org.apache.hadoop.fs.Path(jarFile);
//		org.apache.hadoop.fs.Path pathHDFS = new org.apache.hadoop.fs.Path(remoteFile);
//		if (dfs.exists(pathHDFS)) {
//			FileStatus remoteFs = dfs.getFileStatus(pathHDFS);
//			FileStatus localFs = lfs.getFileStatus(pathLocal);
//			
//			// check if lengths match and modification time is newer
//			// on s3, then we assume they are identical and skip copying			
//			if (remoteFs.getLen() == localFs.getLen() &&
//					remoteFs.getModificationTime() >= localFs.getModificationTime()) {
//				System.out.println("Newer file of same size already exists on S3, skipping copying");
//			} else {
//				dfs.delete(pathHDFS, true);
//				System.out.println("Copying " + jarFile + " to " + prefix);
//				dfs.copyFromLocalFile(pathLocal, pathHDFS);
//			}
//		} else {
//			System.out.println("Copying " + jarFile + " to " + prefix);
//			dfs.copyFromLocalFile(pathLocal, pathHDFS);
//		}
//		
//		// 3. submit job flow on EMR
//		
//		//    args: hadoop ...
//		//    this will run this with hadoop mode on EMR
//	}
//
//	private static void info(Class<? extends ProcessModule> moduleClazz,
//			String[] args) throws Exception {
//		ProcessModule module = moduleClazz.newInstance();
//		module.printInfo();
//	}
//
//	// copied from hadoop JobConf
//	private static String findContainingJar(Class<?> my_class) {
//		ClassLoader loader = my_class.getClassLoader();
//		String class_file = my_class.getName().replaceAll("\\.", "/")
//				+ ".class";
//		try {
//			for (Enumeration<URL> itr = loader.getResources(class_file); itr
//					.hasMoreElements();) {
//				URL url = (URL) itr.nextElement();
//				if ("jar".equals(url.getProtocol())) {
//					String toReturn = url.getPath();
//					if (toReturn.startsWith("file:")) {
//						toReturn = toReturn.substring("file:".length());
//					}
//					toReturn = URLDecoder.decode(toReturn, "UTF-8");
//					return toReturn.replaceAll("!.*$", "");
//				}
//			}
//		} catch (IOException e) {
//			throw new RuntimeException(e);
//		}
//		return null;
//	}
//}
