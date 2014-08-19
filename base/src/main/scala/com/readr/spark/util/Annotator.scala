package com.readr.spark.util

import scala.io.Source
import scala.collection.mutable._
import org.apache.hadoop.io._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.join.CompositeInputFormat;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapred.lib.IdentityReducer;

import java.util.Properties

import com.readr.spark.common._

class Annotator(
  val generates:Array[Class[_]], 
  val requires:Array[Class[_]]) extends java.io.Serializable {
  
  def annotate(ins:Any*):Array[Any] = ???
	
  def init = {}

  def close = {}

  protected var reporter:ProgressReporter = null

  protected def setProgressReporter(reporter:ProgressReporter) = {
	this.reporter = reporter
  }
}