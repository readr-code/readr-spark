package com.readr.spark.util

import org.apache.spark.SparkContext

/**
 * Created by raphael on 9/30/14.
 */
class Operation {

  def init = {}

  def requires:Array[String] = ???

  //def run(params:Map[String,String]):Unit = ???

  def run(params:String*)(implicit sc:SparkContext):Unit = ???
}
