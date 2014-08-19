//package com.readr.spark.util
//
//import org.apache.spark.SparkContext._
//import org.apache.spark.rdd._
//
//import scala.io.Source
//import scala.collection.mutable._
//import org.apache.hadoop.io._
//import org.apache.spark._
//import org.apache.spark.SparkContext._
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.BytesWritable;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.mapred.FileOutputFormat;
//import org.apache.hadoop.mapred.JobClient;
//import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapred.MapReduceBase;
//import org.apache.hadoop.mapred.Mapper;
//import org.apache.hadoop.mapred.OutputCollector;
//import org.apache.hadoop.mapred.Reporter;
//import org.apache.hadoop.mapred.RunningJob;
//import org.apache.hadoop.mapred.SequenceFileInputFormat;
//import org.apache.hadoop.mapred.SequenceFileOutputFormat;
//import org.apache.hadoop.mapred.join.CompositeInputFormat;
//import org.apache.hadoop.mapred.join.TupleWritable;
//import org.apache.hadoop.mapred.lib.IdentityReducer;
//
//import java.util.Properties
//
//import com.readr.spark.common._
//
//class FrameCreator extends java.io.Serializable {
//  
//  rddFrame:RDD[(Int,Frame)]
//  (Int,Seq[FrameMatchFeature])
//  
//  def run(ins:Any*):Array[Any] = ???
//	
//  // 3 args: annotatedDocs, frames, frameMatchFeatures,
//  // output (same three sets?)
//  def run(rdd)
//  
//  def init = {}
//
//  def close = {}
//
//  protected var reporter:ProgressReporter = null
//
//  protected def setProgressReporter(reporter:ProgressReporter) = {
//	this.reporter = reporter
//  }
//}