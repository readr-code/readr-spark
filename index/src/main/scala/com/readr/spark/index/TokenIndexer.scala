package com.readr.spark.index

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

import scala.collection.mutable._
import scala.collection.JavaConversions._

import com.readr.model._
import com.readr.model.annotation._
import com.readr.spark.util.Utils._


object TokenIndexer {
  
  def toTokens(t:TextAnn, toa:TokenOffsetAnn):TokensAnn =
    TokensAnn(toTokens(t.text, toa.tokens))
  
  def toTokens(text:String, tos:Array[Offsets]):Array[String] =
    tos.map(x => text.substring(x.f, x.t).replaceAll("\n", " "))
  
  def offsets2text(off:Array[Offsets]):String =
    off.map(x => x.f.toString + ":" + x.t.toString).mkString(" ")
  
  // NOTE: if you want to join array strings, run arr.mkString(" ")
  
  // assume RDD format (id, TextAnn, TokenOffsetsAnn, SentenceOffsetsAnn, SentenceTokenOffsetsAnn)
  def run(outDir:String, rdd:RDD[(Long,Array[Any])], textAnnID:Int, tokenOffsetAnnID:Int, 
      tokensAnnID:Int, sentenceOffsetAnnID:Int, sentenceTokenOffsetAnnID:Int)
  	(implicit sc:SparkContext):Unit = {

    val sentenceTokenized = rdd.flatMap(x => {
        val l = new ArrayBuffer[(Long,Int,String)]()
        val id = x._1
        val textAnn = x._2(textAnnID).asInstanceOf[TextAnn]
        val tokenOffsetAnn = x._2(tokenOffsetAnnID).asInstanceOf[TokenOffsetAnn]
        val sentenceTokenOffsetAnn = x._2(sentenceTokenOffsetAnnID).asInstanceOf[SentenceTokenOffsetAnn]
        val tokens = toTokens(textAnn, tokenOffsetAnn).tokens
        for (i <- 0 until sentenceTokenOffsetAnn.sents.length) {
          val s = sentenceTokenOffsetAnn.sents(i)
          l += Tuple3(id, i, tokens.slice(s.f, s.t).mkString(" "))
        }
        l
      }
    )

    val sentenceTextToken = rdd.flatMap(x => {
        val l = new ArrayBuffer[(Long,Int,String,String)]
        val id = x._1
        val textAnn = x._2(textAnnID).asInstanceOf[TextAnn]
        val tokenOffsetAnn = x._2(tokenOffsetAnnID).asInstanceOf[TokenOffsetAnn]
        val sentenceTokenOffsetAnn = x._2(sentenceTokenOffsetAnnID).asInstanceOf[SentenceTokenOffsetAnn]
        val sentenceOffsetAnn = x._2(sentenceOffsetAnnID).asInstanceOf[SentenceOffsetAnn]
        for (i <- 0 until sentenceTokenOffsetAnn.sents.length) {
          val to = sentenceTokenOffsetAnn.sents(i)
          val co = sentenceOffsetAnn.sents(i)
          val sentenceText = escape(textAnn.text.substring(co.f, co.t))
          val tokenOffsets = offsets2text(tokenOffsetAnn.tokens.slice(to.f, to.t))
          l += Tuple4(id, i, sentenceText, tokenOffsets)
        }
        l
      }
    )
    
    val sentence = rdd.flatMap(x => {
        val l = new ArrayBuffer[(Long,Int,Int,Int,Int,Int)]
        val id = x._1
        val sentenceTokenOffsetAnn = x._2(sentenceTokenOffsetAnnID).asInstanceOf[SentenceTokenOffsetAnn]
        val sentenceOffsetAnn = x._2(sentenceOffsetAnnID).asInstanceOf[SentenceOffsetAnn]
        for (i <- 0 until sentenceTokenOffsetAnn.sents.length) {
          val to = sentenceTokenOffsetAnn.sents(i)
          val co = sentenceOffsetAnn.sents(i)
          l += Tuple6(id, i, co.f, co.t, to.f, to.t)
        }
        l
      }
    )
    
    val allTokens = rdd.flatMap(x => {
        val textAnn = x._2(textAnnID).asInstanceOf[TextAnn]
        val tokenOffsetAnn = x._2(tokenOffsetAnnID).asInstanceOf[TokenOffsetAnn]      
        toTokens(textAnn, tokenOffsetAnn).tokens        
      }
    ).distinct
    
    val token2name = allTokens.repartition(1).mapPartitions(x => { 
      var c = 0
      x.map(t => {
        val tup = (c, t)
        c += 1
        tup
      })
    })
    
    val tokenInst2basic = rdd.flatMap(x => {
        val l = new ArrayBuffer[(String,(Long,Int,Int))]()
        val id = x._1
        val textAnn = x._2(textAnnID).asInstanceOf[TextAnn]
        val tokenOffsetAnn = x._2(tokenOffsetAnnID).asInstanceOf[TokenOffsetAnn]      
        val tokens = toTokens(textAnn, tokenOffsetAnn).tokens
        val sentenceTokenOffsetAnn = x._2(sentenceTokenOffsetAnnID).asInstanceOf[SentenceTokenOffsetAnn]
        for (i <- 0 until sentenceTokenOffsetAnn.sents.length) {
          val s = sentenceTokenOffsetAnn.sents(i)
          for (j <- s.f until s.t)
            l += Tuple2(tokens(j), Tuple3(id, i, j)) //token, documentID, sentenceID, pos
        }
        l      
    }).join(token2name.map(_.swap)).map(x => {
      val t =x._2._1
      //(t._1, t._2, t._3, x._2._2) // documentID, sentenceID, pos, tokenID
      (x._2._2, t._1, t._3) // tokenID, documentID, pos
    })
    // TODO: sortByKey, put all in pair tuple2 as key and create comparator on keys?
    
    
    val documentTokenOffset = rdd.map(x => {
        val id = x._1
        val tokenOffsetAnn = x._2(tokenOffsetAnnID).asInstanceOf[TokenOffsetAnn]
        (id, offsets2text(tokenOffsetAnn.tokens))
    })
    
    
    
    token2name.map(tsv(_)).saveAsTextFile(outDir + "/token2name")
    sentenceTokenized.map(tsv(_)).saveAsTextFile(outDir + "/sentenceTokenized")
    tokenInst2basic.map(tsv(_)).saveAsTextFile(outDir + "/tokenInst2basic")
    sentenceTextToken.map(tsv(_)).saveAsTextFile(outDir + "/sentenceTextToken")
    sentence.map(tsv(_)).saveAsTextFile(outDir + "/sentence")
    documentTokenOffset.map(tsv(_)).saveAsTextFile(outDir + "/documentTokenOffsets")
  }
  
  
  // finds Source
  def run(outDir:String, rdd:RDD[(Long,Array[Any])])(implicit sc:SparkContext):Unit = {
    val c0 = firstColumnOfType(rdd, classOf[TextAnn])
    val c1 = firstColumnOfType(rdd, classOf[TokenOffsetAnn])
    val c2 = firstColumnOfType(rdd, classOf[TokensAnn])
    val c3 = firstColumnOfType(rdd, classOf[SentenceOffsetAnn])
    val c4 = firstColumnOfType(rdd, classOf[SentenceTokenOffsetAnn])
    run(outDir, rdd, c0, c1, c2, c3, c4)
  }

}
