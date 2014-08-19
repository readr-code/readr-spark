package com.readr.spark.other

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import scala.collection.JavaConversions._

import com.readr.model._
import com.readr.model.annotation._


import com.readr.spark.util.Annotator

import java.util._

import scala.collection.mutable._

object SimpleNERSegmenter {
  
}

class SimpleNERSegmenter extends Annotator(
      generates = Array(classOf[NERAnn]),
      requires = Array(classOf[NERTagAnn], classOf[SentenceTokenOffsetAnn], classOf[ConstituentAnn])) {
  
  val properties = new Properties()
  
  override def annotate(ins:Any*):Array[Any] = {
    Array(run(ins(0).asInstanceOf[NERTagAnn], 
        ins(1).asInstanceOf[SentenceTokenOffsetAnn],
        ins(2).asInstanceOf[ConstituentAnn]))
  }
  
  def run(nerta:NERTagAnn, stoa:SentenceTokenOffsetAnn, ca:ConstituentAnn):NERAnn = {
	val all = ArrayBuffer[NamedEntity]()
	for (sentNum <- 0 until stoa.sents.length) {
	  val sto = stoa.sents(sentNum)
	  val segments = ArrayBuffer[NamedEntity]()
	    
      // first part: find sequences of identical NER tags and segment
	  var lastTag = nerta.tokens(sto.f)
	  var lastTagBegin = sto.f
	  for (i <- sto.f+1 until sto.t) {
	    val tag = nerta.tokens(i)
	      
	    if (!tag.equals(lastTag)) {
	      if (!lastTag.equals("O")) 
	        segments += NamedEntity(lastTag, Offsets(lastTagBegin, i))
	      lastTag = tag
		  lastTagBegin = i
	    }
	  }
	  if (!lastTag.equals("O")) 
	    segments += NamedEntity(lastTag, Offsets(lastTagBegin, sto.t))
	      
	    
	  // second part (optional): add head information from constituents
      val segmentsWithHeads = ArrayBuffer[NamedEntity]()
	  for (seg <- segments) {
		// find (smallest) NP that fully contains named entity
		var s:Constituent = null // smallest
		for (con <- ca.sents(sentNum)) {
		  if (con.offsets.f <= seg.offsets.f && con.offsets.t >= seg.offsets.t &&
			(s == null || con.offsets.t - con.offsets.f < s.offsets.t - s.offsets.f)) s = con 						
		}
		// note: head might not be inside start end for the ner,
		// in this case we pick the last token in segment as head
		val head = 
		  if (s != null && s.head >= seg.offsets.f && s.head < seg.offsets.t) s.head else seg.offsets.t - 1
		    
		segmentsWithHeads += seg.copy(head = head)
	  }
	  all ++= segmentsWithHeads //.toArray
	}
	NERAnn(all.toArray)	  
  }
}
