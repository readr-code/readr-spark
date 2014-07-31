package com.readr.spark.allenai

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import com.readr.model._
import com.readr.model.annotation._

import com.readr.spark.util.Annotator


import java.util._

object MorphaStemmer {
//  def toAllenai(to:TokensAnn, soa:TokenOffsetAnn):Seq[org.allenai.nlpstack.tokenize.Token] =
//    for (i <- 0 until to.tokens.length) yield new org.allenai.nlpstack.tokenize.Token(to.tokens(i), soa.tokens(i).f)
//
//  def fromAllenai(seq:Iterable[org.allenai.nlpstack.tokenize.Token]):TokenOffsetAnn =
//    TokenOffsetAnn(seq.map(x => Offsets(x.offset, x.offset + x.string.length)).toArray)  
}

class MorphaStemmer extends Annotator(
      generates = Array(classOf[LemmaAnn]),
      requires = Array(classOf[TokensAnn], classOf[POSAnn])) {
  
  @transient lazy val stemmer = new org.allenai.nlpstack.lemmatize.MorphaStemmer
  
  override def annotate(ins:Any*):Array[Any] = {
    Array(run(ins(0).asInstanceOf[TokensAnn],
        ins(1).asInstanceOf[POSAnn]))
  }
  
  def run(ta:TokensAnn, pa:POSAnn):LemmaAnn = {

    val stems = new Array[String](ta.tokens.size)
    for (i <- 0 until ta.tokens.size) {
      val token = ta.tokens(i)
      val postag = pa.pos(i)
      try {
        stems(i) = stemmer.stem(token, postag)
      } catch {
        case e:Exception => println(e.getMessage)
        stems(i) = token
      }
    }
    LemmaAnn(stems)
  }
}
