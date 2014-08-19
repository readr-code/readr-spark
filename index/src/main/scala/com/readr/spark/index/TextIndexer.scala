package com.readr.spark.index

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

import scala.util.control.Breaks._
import scala.collection.mutable._
import scala.collection.JavaConversions._

import com.readr.model._
import com.readr.model.annotation._
import com.readr.spark.util.Utils._


object TextIndexer {
  
  // requires: index of TextAnn
  def run(outDir:String, rdd:RDD[(Long,Array[Any])], ta:Int)(implicit sc:SparkContext):Unit = {
    val t = rdd.map(x => (x._1,x._2(ta).asInstanceOf[TextAnn]))
    val u = t.map(x => "" + x._1 + "\t" + escape(x._2.text))
    u.saveAsTextFile(outDir + "/text")
  }
  
  // finds TextAnn
  def run(outDir:String, rdd:RDD[(Long,Array[Any])])(implicit sc:SparkContext):Unit = {
    val c = firstColumnOfType(rdd, classOf[TextAnn])
    run(outDir, rdd, c)
  }

}
