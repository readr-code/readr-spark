package com.readr.spark.frame

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.collection.mutable.HashSet

import scala.collection.JavaConversions._

import com.readr.model._
import com.readr.model.annotation._
import com.readr.model.frame._


import com.readr.spark.util.Annotator

import scala.collection.mutable.ArrayBuffer
import java.util.Properties
import java.util.Vector

import com.readr.spark.util.Utils._


object MentionFrameCreatorWithCoreference {
  
  def run(rddFrame:RDD[(Long,Frame,Seq[FrameMatchFeature])], rddDoc:RDD[(Long,Array[Any])],
     textAnnID:Int, tokenOffsetAnnID:Int, mentionAnnID:Int, coreferenceAnnID:Int 
     )(implicit sc:SparkContext):(RDD[(Long,Frame,Seq[FrameMatchFeature])], 
          RDD[(Long,Array[Any])]) = {

    val mentionWithSpans = rddDoc.flatMap(x => {
        val l = new ArrayBuffer[(String,(Long,Int))]()
        val id = x._1
        val textAnn = x._2(textAnnID).asInstanceOf[TextAnn]
        val tokenOffsetAnn = x._2(tokenOffsetAnnID).asInstanceOf[TokenOffsetAnn]
        val mentionAnn = x._2(mentionAnnID).asInstanceOf[MentionAnn]
        val coreferenceAnn = x._2(coreferenceAnnID).asInstanceOf[CoreferenceAnn]
        // count mentions in clusters
        //val counts = Map[Int,Int]()
        for (c <- coreferenceAnn.chains) {
          val m = mentionAnn.mentions(c.representativeMentionNum)
          
          val span = textAnn.text.substring(tokenOffsetAnn.tokens(m.tokenOffsets.f).f, 
              tokenOffsetAnn.tokens(m.tokenOffsets.t - 1).t)
          
          val shortSpan = if (span.length > 100) span.substring(0, 100) else span
          
          for (mentionNum <- c.mentionNums) {
            val head = mentionAnn.mentions(mentionNum).head
            l += Tuple2(shortSpan, Tuple2(x._1, head))
          }
        }
        l
      }
    ).groupByKey //.reduceByKey(_ + _)
    
    
    // each span becomes a frame, give it a unique ID
    val lastFrameID = rddFrame.count    
    val withID = mentionWithSpans.repartition(1).mapPartitions(x => { 
      var c = 0
      x.map(t => {
        val tup = (lastFrameID + c, t._1, t._2)
        c += 1
        tup
      })
    })
    
    // create the frame and it's frameMatchFeatures
    val newRddFrame = withID.map(x => {
      val frameID = x._1.toInt
      val span = x._2
      
      val f = Frame(
          name = span,
          description = "",
          examples = "",
          typ = FrameType.Verb,
          properties = Nil,
          args = Seq(FrameArg(-1, frameID, 0, "a", "", true)),
          valences = Seq(FrameValence(frameID, -1, "mentionSpan(a, \"" + span + "\"")))
      
      val s = ArrayBuffer[FrameMatchFeature]()
      for (it <- x._3) {
        val documentID = it._1.toInt
        val pos = it._2
        
        s += FrameMatchFeature(-1, frameID, true, 0, Seq(FrameMatchFeatureArg(-1, documentID, pos)))
      }
      (frameID.toLong,f,s.toSeq)
    }
    )
    (newRddFrame, rddDoc)
  }
  
    // finds Source
  def run(rddFrame:RDD[(Long,Frame,Seq[FrameMatchFeature])], rddDoc:RDD[(Long,Array[Any])])(implicit sc:SparkContext):(RDD[(Long,Frame,Seq[FrameMatchFeature])], 
          RDD[(Long,Array[Any])]) = {
    val c0 = firstColumnOfType(rddDoc, classOf[TextAnn])
    val c1 = firstColumnOfType(rddDoc, classOf[TokenOffsetAnn])
    val c2 = firstColumnOfType(rddDoc, classOf[MentionAnn])
    val c3 = firstColumnOfType(rddDoc, classOf[CoreferenceAnn])
    run(rddFrame, rddDoc, c0, c1, c2, c3)
  }

}