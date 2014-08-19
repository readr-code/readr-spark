//package com.readr.spark.cli;
//
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.Map;
//
//public class OptionsParser {
//
//	private String[] remArgs;
//	private Map<String,String> map = new HashMap<String,String>();
//	
//	public OptionsParser() {
//		
//	}
//	
//	public void parse(String[] args) {
//		int argPos = 0;
//		for (; argPos < args.length; argPos++) {
//			String kv = args[argPos];
//			if (kv.startsWith("--")) {
//				String key = kv;
//				String val = null;
//				int kvi = kv.indexOf("=");
//				if (kvi > 0) {
//					key = kv.substring(0, kvi);
//					val = kv.substring(kvi+1);
//				}
//				map.put(key, val);
//			} else {
//				break;
//			}
//		}
//		this.remArgs = Arrays.copyOfRange(args, argPos, args.length);
//	}
//	
//	public boolean hasOption(String name) {
//		return map.containsKey(name);
//	}
//	
//	public String getOptionValue(String name) {
//		return map.get(name);
//	}
//	
//	public String getOptionValue(String name, String defaultValue) {
//		String v = map.get(name);
//		if (v == null) v = defaultValue;
//		return v;
//	}
//	
//	public int getOptionIntValue(String name, int defaultValue) {
//		String v = map.get(name);
//		if (v != null) {
//			Integer iv = Integer.parseInt(v);
//			if (iv != null)
//				return iv;
//		}
//		return defaultValue;
//	}
//	
//	public String[] getRemainingArgs() {
//		return remArgs;
//	}
//}
