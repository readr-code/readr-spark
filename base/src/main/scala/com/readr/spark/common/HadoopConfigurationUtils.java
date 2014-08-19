//package com.readr.spark.common;
//
//import java.util.Iterator;
//import java.util.Map.Entry;
//import java.util.Properties;
//
//import org.apache.commons.configuration.PropertiesConfiguration;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//
//import com.readr.common.ConfigurationManager;
//
//
//public class HadoopConfigurationUtils {
//
//	static String[] HADOOP_CONFIG_FILES = {
//		"src/core/core-default.xml",
//		"src/hdfs/hdfs-default.xml",
//		"src/mapred/mapred-default.xml",
//		"conf/core-site.xml",
//		"conf/hdfs-site.xml",
//		"conf/mapred-site.xml"
//	};
//
//	public static Configuration getConfiguration() throws Exception {
//		PropertiesConfiguration c = ConfigurationManager.get().getConfiguration();
//
//		Configuration conf = new Configuration();
//		String HADOOP_HOME = c.getString("HADOOP_HOME");
//		if (HADOOP_HOME != null && !HADOOP_HOME.equals("")) {
//			for (String cf : HADOOP_CONFIG_FILES)
//				conf.addResource(new Path(HADOOP_HOME, cf));
//		}
//
//		// add additional variables
//		Iterator<String> it = c.getKeys("HADOOP");
//		while (it.hasNext()) {
//			String key = it.next();
//			String name = key.substring("HADOOP.".length());
//			conf.set(name, c.getString(key));
//		}
//		
//		return conf;
//	}
//	
//	public static Properties getConfigurationAsProperties(Configuration c) throws Exception {
//		Properties p = new Properties();
////		Configuration c = getConfiguration();
//		Iterator<Entry<String,String>> it = c.iterator();
//		while (it.hasNext()) {
//			Entry<String,String> e = it.next();
//			p.put(e.getKey(), e.getValue());
//		}
//		return p;
//	}
//}
