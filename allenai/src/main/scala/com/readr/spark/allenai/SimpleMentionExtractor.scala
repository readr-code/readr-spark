package com.readr.spark.allenai

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.mutable.HashSet

import scala.collection.JavaConversions._

import com.readr.model._
import com.readr.model.annotation._

import com.readr.spark.util.Annotator

import scala.collection.mutable._
import java.util.Properties
import java.util.Vector

object SimpleMentionExtractor {
  
}

class SimpleMentionExtractor extends Annotator(
      generates = Array(classOf[MentionAnn]),
      requires = Array(classOf[TextAnn], classOf[TokenOffsetAnn], classOf[SentenceTokenOffsetAnn], 
          classOf[SentenceDependencyAnn], classOf[POSAnn])) {
  
  val properties = new Properties()
  
  override def annotate(ins:Any*):Array[Any] = {
    val out = run(ins(0).asInstanceOf[TextAnn],
        ins(1).asInstanceOf[TokenOffsetAnn], 
        ins(2).asInstanceOf[SentenceTokenOffsetAnn], 
        ins(3).asInstanceOf[SentenceDependencyAnn],
        ins(4).asInstanceOf[POSAnn])
    Array(out)
  }
  
  def run(ta:TextAnn, toa:TokenOffsetAnn, stoa:SentenceTokenOffsetAnn, sda:SentenceDependencyAnn, 
      pa:POSAnn):MentionAnn = {

    // identify NP chunks according to patterns
	val l = new ArrayBuffer[Mention]()
    
	var mentionNum = 0
    for (sentNum <- 0 until stoa.sents.size) {
      
      // each token could be the start of a NP chunk
      val so = stoa.sents(sentNum)
      val sd = sda.sents(sentNum)
      
      val depIndex = new Array[ArrayBuffer[Dependency]](so.t - so.f)
      for (i <- 0 until depIndex.length) depIndex(i) = new ArrayBuffer[Dependency]()
      for (d <- sd) {
        if (d.to == -1) {
          println(ta.text)
          println(sentNum)
          println("WOW: Dependency with d.to -1")
          println(d)
          ta
          
        }
        depIndex(d.to) += d
        
      }
      var tokStart = so.f
      while (tokStart < so.t) {
        var tokEnd = tokStart
        while (tokEnd < so.t && pa.pos(tokEnd).startsWith("NN")) tokEnd += 1
        // got range tokStart-tokEnd
        if (tokEnd > tokStart) {
          l += Mention(
              mentionNum,
              tokEnd-1,
              Offsets(tokStart, tokEnd),
              Mention.UNKNOWN, // typ
              Mention.UNKNOWN, // number
              Mention.UNKNOWN, // gender
              Mention.UNKNOWN // animacy
          )
          mentionNum += 1
        }
        tokStart = tokEnd + 1
      }
    }
	MentionAnn(l.toArray)
  }
}