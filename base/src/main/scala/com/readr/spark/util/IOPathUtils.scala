package com.readr.spark.util

import scala.io.Source
import scala.collection.mutable._
import org.apache.hadoop.io._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path;

object IOPathUtils {
 
  def getInputPaths(argDir:String, outIn:Array[String], defaultOutIn:Array[String]):Array[Path] = {
	val inputPaths = new Array[Path](defaultOutIn.length - 1)
	for (i <- 0 until inputPaths.length) {
	  var name = defaultOutIn(i+1)
	  if (outIn != null && outIn.length > i+1) name = outIn(i+1)
		inputPaths(i) = new Path(argDir + "/" + name)
	}
	inputPaths
  }

  def getOutputPath(argDir:String, outIn:Array[String], defaultOutIn:Array[String]):Path = {
	var name = defaultOutIn(0)
	if (outIn != null && outIn.length > 0) name = outIn(0)
	val outputPath = new Path(argDir + "/" + name)
	outputPath;
  }	

  def getDefaultOutIn(annotator:Annotator):Array[String] = {
	val outputType = annotator.generates
	val inputTypes = annotator.requires
	val defaultOutIn = new Array[String](inputTypes.length + 1)
	defaultOutIn(0) = "doc." + outputType(0).getSimpleName() //hack
	for (i <- 0 until inputTypes.length) {
	  val name = "doc." + inputTypes(i).getSimpleName()
	  defaultOutIn(i+1) = name
	}
	defaultOutIn
  }
	
  def getDefaultOutInText(annotator:Annotator):Array[String] = {
	val defaultOutIn = getDefaultOutIn(annotator)
	for (i <- 0 until defaultOutIn.length) defaultOutIn(i) += "Text"
	  return defaultOutIn;
  }
	
  def logInputOutputPaths(outputPath:Path, inputPaths:Array[Path]) = {
	System.out.println("Output: " + outputPath.toUri().toString());
	for (ip <- inputPaths)
	  println("Input:  " + ip.toUri().toString())
  }  
}