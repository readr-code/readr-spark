//package com.readr.spark.cli;
//
//import java.io.InputStream;
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.Map;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.pig.ExecType;
//import org.apache.pig.PigServer;
//
//import com.readr.hadoop.annotate.Annotate;
//import com.readr.hadoop.annotate.AnnotateFast;
//import com.readr.hadoop.annotate.AnnotateText;
//import com.readr.hadoop.annotate.AnnotateTextFast;
//import com.readr.hadoop.common.AbstractAnnotator;
//import com.readr.hadoop.common.AbstractHadoop;
//import com.readr.hadoop.module.AnnotateProcessModule;
//import com.readr.hadoop.module.HadoopProcessModule;
//import com.readr.hadoop.module.PigProcessModule;
//import com.readr.hadoop.module.ProcessModule;
//
//public class CLIrun {
//
//	static int DEFAULT_NUM_PARTITIONS=1;
//	
//	public static void run(String jarFile, boolean local, Configuration configuration, 
//			Class<? extends ProcessModule> moduleClazz,
//			String[] args) throws Exception {
//		
//		// if it is a PIG script
//		if (PigProcessModule.class.isAssignableFrom(moduleClazz))
//			runPig(jarFile, local, configuration, moduleClazz, args);
//		// else if it is a document annotator
//		else if (AnnotateProcessModule.class.isAssignableFrom(moduleClazz))
//			runAnnotate(jarFile, local, configuration, moduleClazz, args);
//		else if (HadoopProcessModule.class.isAssignableFrom(moduleClazz))
//			runHadoop(jarFile, local, configuration, moduleClazz, args);
//		else //if (CustomProcessModule.class.isAssignableFrom(moduleClazz))
//			//runCustom(configuration, moduleClazz, args);
//			throw new Exception("module must be of type Pig or Annotate");
//		return;
//	}
//
//	public static void usagePig() {
//		System.out.println("Usage: ... DIR [<out> [<in1> [<in2> [...]]]");
//		System.out.println("where DIR is the input/output directory and can be on one of the ");
//		System.out.println("file://, hdfs://, and s3n:// filesystems.");
//		System.out.println("If <out> and <in1> are specified, the system reads from DIR/<in1>");
//		System.out.println("and writes to DIR/<out>. If they are not specified, default names");
//		System.out.println("of the form DIR/doc.TYPE are assumed.");
//	}
//
//	public static void usageAnnotate() {
//		System.out.println("Usage: ... [--partitions=<num>] [--reduce] [--text] DIR [<out> [<in1> [<in2> [...]]]");
//		System.out.println("where DIR is the input/output directory and can be on one of the ");
//		System.out.println("file://, hdfs://, and s3n:// filesystems.");
//		System.out.println("If <out> and <in1> are specified, the system reads from DIR/<in1>");
//		System.out.println("and writes to DIR/<out>. If they are not specified, default names");
//		System.out.println("of the form DIR/doc.TYPE are assumed.");
//		System.out.println("   --partitions=<num>   Number of chunks, must be the same for all inputs and output.");
//		System.out.println("   --reduce             By default, readr uses total order partitioning for all doc data");
//		System.out.println("                        which allows it to skip the reduce phases after each mapper, use");
//		System.out.println("                        this switch to not make total order assumption and run reduce.");
//		System.out.println("   --text               All inputs and outputs are in text (rather than the default ");
//		System.out.println("                        binary protobuf) ");
//	}
//	
//	public static void usageHadoop() {
//		System.out.println("Usage: ... DIR [<out> [<in1> [<in2> [...]]]");
//		System.out.println("where DIR is the input/output directory and can be one of the ");
//		System.out.println("file://, hdfs://, and s3n:// filesystems.");
//		System.out.println("If <out> and <in1> are specified, the system reads from DIR/<in1>");
//		System.out.println("and writes to DIR/<out>. If they are not specified, default names");
//		System.out.println("of the form DIR/doc.TYPE are assumed.");
//	}
//
//	public static void runPig( 
//			String jarFile,
//			boolean local,
//			Configuration configuration,
//			Class<? extends ProcessModule> moduleClazz, 
//			String[] args) throws Exception {
//
//		OptionsParser parser = new OptionsParser();
//		parser.parse(args);
//		args = parser.getRemainingArgs();
//
//		if (args.length < 1) {
//			usageAnnotate();
//			return;
//		}
//		String dir = args[0];
//		
//		// TODO: overwrite
//		String[] outIn = Arrays.copyOfRange(args, 1, args.length);
//
//		ProcessModule module = moduleClazz.newInstance();
//		
//		//Properties props = HadoopConfigurationUtils.getConfigurationAsProperties(configuration);
//		//String jobTracker = props.getProperty("mapred.job.tracker");
//
//		//ExecType et = jobTracker.equals("local")? ExecType.LOCAL : ExecType.MAPREDUCE;
//
//		ExecType et = ExecType.MAPREDUCE;
//		if (local) et = ExecType.LOCAL;
//		
//		PigServer pigServer = new PigServer(et); //, props);
//		// register current jar, problem is hadoop is first running unjar, and this points to a folder with classes
//		
//		//System.out.println("registering jar");
//		//System.out.println(CLIrun.class.getProtectionDomain().getCodeSource().getLocation().toURI().toString());
//		//pigServer.registerJar(CLIrun.class.getProtectionDomain().getCodeSource().getLocation().toURI().toString());
//		//pigServer.registerJar("/Users/raphael/readr/module/hadoop-tokenIndex/target/hadoop-tokenIndex-1.0-SNAPSHOT-jar-with-dependencies.jar");
//		//pigServer.registerJar("s3n://raphu/code/hadoop-tokenIndex-1.0-SNAPSHOT-jar-with-dependencies.jar");
//		System.out.println("registerJar " + jarFile);
//		pigServer.registerJar(jarFile);
//		
////        Map<String, String> env = System.getenv();
////        for (String envName : env.keySet()) {
////            System.out.format("%s=%s%n",
////                              envName,
////                              env.get(envName));
////        }
//		
//		
//		Map<String,String> params = new HashMap<String,String>();
//		params.put("dir", dir);
//
//		pigServer.setBatchOn();
//		//pigServer..debugOff();
//		
//		for (String pigScript : module.getProcessorPigScripts()) {
//			System.out.println("adding pigscript " + pigScript);
//			InputStream in = moduleClazz.getClassLoader().getResourceAsStream(pigScript);
//			pigServer.registerScript(in, params);
//		}
//		pigServer.executeBatch();
//		pigServer.shutdown();
//	}
//	
////	private static void printInputStream(InputStream in) throws Exception {
////		BufferedReader r = new BufferedReader(new InputStreamReader(in));
////		String l = null;
////		while ((l = r.readLine())!= null)
////			System.out.println(l);
////		r.close();
////	}
//	
//	public static void runAnnotate(
//			String jarFile,
//			boolean local, 
//			Configuration configuration, 
//			Class<? extends ProcessModule> moduleClazz, 
//			String[] args) throws Exception {
//		
//		OptionsParser parser = new OptionsParser();
//		parser.parse(args);
//		args = parser.getRemainingArgs();
//
//		if (args.length < 1) {
//			usageAnnotate();
//			return;
//		}
//		String dir = args[0];
//		String[] outIn = Arrays.copyOfRange(args, 1, args.length);
//
//		int partitions = parser.getOptionIntValue("--partitions", DEFAULT_NUM_PARTITIONS);
//		boolean reduce = parser.hasOption("--reduce");
//		boolean text = parser.hasOption("--text");
//		boolean overwrite = parser.hasOption("--overwrite");
//
//		// get annotator
//		ProcessModule module = moduleClazz.newInstance();
//		Class<? extends AbstractAnnotator> annotatorClazz =
//				module.getProcessorAnnotator();
//		System.out.println(annotatorClazz.getName());
//		
//		if (reduce && !text) {
//			Annotate.execute(configuration, dir, partitions, annotatorClazz, outIn, overwrite);
//		} else
//		if (reduce && text) {
//			AnnotateText.execute(configuration, dir, partitions, annotatorClazz, outIn, overwrite);
//		} else
//		if (!reduce && !text) {
//			AnnotateFast.execute(configuration, dir, partitions, annotatorClazz, outIn, overwrite);
//		} else
//		if (!reduce && text) {
//			AnnotateTextFast.execute(configuration, dir, partitions, annotatorClazz, outIn, overwrite);
//		}
//	}
//	
//	public static void runHadoop(
//			String jarFile,
//			boolean local,
//			Configuration configuration, 
//			Class<? extends ProcessModule> moduleClazz, 
//			String[] args) throws Exception {
//		
//		ProcessModule module = moduleClazz.newInstance();
//		Class<? extends AbstractHadoop> hadoopClazz =
//				module.getProcessorHadoop();
//		System.out.println(hadoopClazz.getName());
//
//		AbstractHadoop hadoop = hadoopClazz.newInstance();
//		
//		hadoop.run(configuration, args);
//	}
//}
