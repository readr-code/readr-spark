//package com.readr.spark.cli;
//
//import java.io.InputStream;
//import java.util.ArrayList;
//import java.util.Iterator;
//import java.util.List;
//
//import org.apache.commons.configuration.PropertiesConfiguration;
//
//import com.readr.hadoop.common.AbstractAnnotator;
//import com.readr.hadoop.common.AbstractHadoop;
//
//public abstract class ProcessModule {
//
//	private Class<? extends AbstractAnnotator> annotatorClazz;
//	private List<String> pigScripts =  new ArrayList<String>();
//	private Class<? extends AbstractHadoop> hadoopClazz;
//	
//	protected ProcessModule() {
//		init();
//	}
//	
//	public abstract void init();
//	
//	public void setProcessorAnnotator(Class<? extends AbstractAnnotator> annotatorClazz) {
//		this.annotatorClazz = annotatorClazz;
//	}
//	
//	public void addProcessorPigScript(String pigScript) {
//		this.pigScripts.add(pigScript);
//	}
//
//	public void setProcessorHadoop(Class<? extends AbstractHadoop> hadoopClazz) {
//		this.hadoopClazz = hadoopClazz;
//	}
//	
//	public Class<? extends AbstractAnnotator> getProcessorAnnotator() {
//		return annotatorClazz;
//	}
//	
//	public List<String> getProcessorPigScripts() {
//		return pigScripts;
//	}
//	
//	public Class<? extends AbstractHadoop> getProcessorHadoop() {
//		return hadoopClazz;
//	}
//	
//	public void printInfo() throws Exception {
//		InputStream is = getClass().getClassLoader().getResourceAsStream("module.properties");
//		PropertiesConfiguration configuration = new PropertiesConfiguration();
//		configuration.load(is);
//		Iterator<String> it = configuration.getKeys();
//		while (it.hasNext()) {
//			String key = it.next();
//			System.out.println(key + "\t" + configuration.getString(key));
//		}
//	}
//}
