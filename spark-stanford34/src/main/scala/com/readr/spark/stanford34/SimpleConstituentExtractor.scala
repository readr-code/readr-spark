package com.readr.spark.stanford34

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


import scala.collection.JavaConversions._

import com.readr.model._
import com.readr.model.annotation._

import edu.stanford.nlp.trees.HeadFinder
import edu.stanford.nlp.trees.ModCollinsHeadFinder
import edu.stanford.nlp.trees.Tree
import edu.stanford.nlp.util.IntPair

import com.readr.spark.util.Annotator

import java.util._

import scala.collection.mutable._

object SimpleConstituentExtractor {
  
}

class SimpleConstituentExtractor extends Annotator(
      generates = Array(classOf[ConstituentAnn]),
      requires = Array(classOf[ParseAnn])) {
  
  val properties = new Properties()
  val hf = new ModCollinsHeadFinder()
  
  override def annotate(ins:Any*):Array[Any] = {
    Array(run(ins(0).asInstanceOf[ParseAnn]))
  }
  
  def run(pa:ParseAnn):ConstituentAnn = {
    val sents = ArrayBuffer[Array[Constituent]]()
    for (s <- pa.sents) {
      val constituents = ArrayBuffer[Constituent]()

      if (s != null && s.length > 0) {
        // parse PTB style string into Stanford's AST representation
		val tree = Tree.valueOf(s)
		tree.setSpans
		for (subtree <- tree) {				
		  if (!subtree.isLeaf) {
		    try {
		  	  //if (subtree.isLeaf()) continue;					
			  val ip:IntPair = subtree.getSpan
			  //System.out.println(subtree.value() + " " + ip.getSource() + " "  + ip.getTarget());
			  val start = ip.getSource
			  val end = ip.getTarget
			  val name = subtree.value
					
		      val head = subtree.headPreTerminal(hf)
			  val hip:IntPair = head.getSpan
						
			  constituents += Constituent(name, Offsets(start, end), hip.getSource)
		    } catch {
		      case e:Exception => e.printStackTrace
		    }
		  }
		}
      }
      sents += constituents.toArray
    }
    ConstituentAnn(sents.toArray)
  }
}
