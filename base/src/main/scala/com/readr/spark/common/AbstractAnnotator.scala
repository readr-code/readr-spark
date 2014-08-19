package com.readr.spark.common
//~/hadoop2/hadoop-1.1.1/bin/hadoop jar target/datahadoop-1.0-SNAPSHOT-job.jar annotateFast --dir=sample2 --partitions=10 --annotator=StanfordCorefAnnotator

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.util.Properties;

abstract class AbstractAnnotator {
  
  var generates:Class[_ <: AbstractAnnotationType[Any]] = null
  var requires:Array[Class[_ <: AbstractAnnotationType[Any]]] = null
	
  private var reporter:ProgressReporter = null
	
  //protected def this(properties:Properties) = {}

  protected def init(generates:Class[_ <: AbstractAnnotationType[Any]],
      requires:Array[Class[_ <: AbstractAnnotationType[Any]]]):Unit = {
	this.generates = generates
	this.requires = requires
  }
  
  protected def init(generates:String, requires:String):Unit = {
	//this.generates = SerDes.classByName(generates);
	//this.requires = SerDes.classesByNames(requires);
  }
		
  def annotate(ins:Any*):Any = {}
	
  def init():Unit = {}

  def close():Unit = {}

  def setProgressReporter(reporter:ProgressReporter) = {
    this.reporter = reporter
  }
}
