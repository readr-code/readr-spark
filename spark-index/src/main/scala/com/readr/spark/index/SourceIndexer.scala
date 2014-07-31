package com.readr.spark.index

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

import scala.collection.mutable._
import scala.collection.JavaConversions._

import com.readr.model._
import com.readr.model.annotation._
import com.readr.spark.util.Utils._

object SourceIndexer {
  
  // requires: index of Source
  def run(outDir:String, rdd:RDD[(Long,Array[Any])], so:Int)(implicit sc:SparkContext):Unit = {
    val t = rdd.map(x => (x._1,x._2(so).asInstanceOf[Source]))
    val u = t.map(x => "" + x._1 + "\t" + escape(x._2.raw))
    u.saveAsTextFile(outDir + "/source")
  }
  
  // finds Source
  def run(outDir:String, rdd:RDD[(Long,Array[Any])])(implicit sc:SparkContext):Unit = {
    val c = firstColumnOfType(rdd, classOf[Source])
    run(outDir, rdd, c)
  }
  
}
