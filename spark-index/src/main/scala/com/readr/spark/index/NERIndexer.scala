package com.readr.spark.index

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

import scala.collection.mutable._
import scala.collection.JavaConversions._

import com.readr.model._
import com.readr.model.annotation._
import com.readr.spark.util.Utils._


object NERIndexer {
  
//  def toTokens(t:TextAnn, toa:TokenOffsetAnn):TokensAnn =
//    TokensAnn(toTokens(t.text, toa.tokens))
//  
//  def toTokens(text:String, tos:Array[Offsets]):Array[String] =
//    tos.map(x => text.substring(x.f, x.t))
//  
//  def offsets2text(off:Array[Offsets]):String =
//    off.map(x => x.f.toString + ":" + x.t.toString).mkString(" ")
  
  // NOTE: if you want to join array strings, run arr.mkString(" ")
  
  
  // assume RDD format (id, NERAnn)
  def run(outDir:String, rdd:RDD[(Long,Array[Any])], nerAnnID:Int)
  	(implicit sc:SparkContext):Unit = {
    
    val ner = rdd.flatMap(x => {
        val l = new ArrayBuffer[(Long,Int,String,Int,Int)]()
        val id = x._1
        val nerAnn = x._2(nerAnnID).asInstanceOf[NERAnn]
        for (ne <- nerAnn.entities)
          l += Tuple5(id, ne.head, ne.typ, ne.offsets.f, ne.offsets.t)
        l
      }
    )

    ner.map(tsv(_)).saveAsTextFile(outDir + "/ner")
  }
  
  
  // finds Source
  def run(outDir:String, rdd:RDD[(Long,Array[Any])])(implicit sc:SparkContext):Unit = {
    val c0 = firstColumnOfType(rdd, classOf[NERAnn])
    run(outDir, rdd, c0)
  }

}
