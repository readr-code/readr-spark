package com.readr.spark.other

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

// puts every mention into its own cluster

object PseudoCoreferenceResolver {
  
}

class PseudoCoreferenceResolver extends Annotator(
      generates = Array(classOf[CoreferenceAnn]),
      requires = Array(classOf[MentionAnn])) {
  
  val properties = new Properties()
  
  override def annotate(ins:Any*):Array[Any] = {
    Array(run(ins(0).asInstanceOf[MentionAnn]))
  }
  
  def run(ma:MentionAnn):CoreferenceAnn = {
    val chains = ma.mentions.map(x =>
      CoreferenceChain(
        chainNum = x.mentionNum, 
        representativeMentionNum = x.mentionNum,
        mentionNums = Array(x.mentionNum))
    )
    CoreferenceAnn(chains)
  }
}