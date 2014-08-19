package com.readr.spark.distsim

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

import com.readr.spark.util.Utils._

import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.SingularValueDecomposition


object LSAOnSentences {

  def toTokens(t:TextAnn, toa:TokenOffsetAnn):TokensAnn =
    TokensAnn(toTokens(t.text, toa.tokens))
  
  def toTokens(text:String, tos:Array[Offsets]):Array[String] =
    tos.map(x => text.substring(x.f, x.t))
  
  def offsets2text(off:Array[Offsets]):String =
    off.map(x => x.f.toString + ":" + x.t.toString).mkString(" ")

  def run(outDir:String, rdd:RDD[(Long,Array[Any])],
     textAnnID:Int, tokenOffsetAnnID:Int, sentenceTokenOffsetAnnID:Int)(implicit sc:SparkContext):Unit = {
    
    // generate tokenID, token, frequency
    val allTokens = rdd.
      flatMap(x => {
        val textAnn = x._2(textAnnID).asInstanceOf[TextAnn]
        val tokenOffsetAnn = x._2(tokenOffsetAnnID).asInstanceOf[TokenOffsetAnn]      
        //toTokens(textAnn, tokenOffsetAnn).tokens.map(Tuple2(_, 1))
        TokensAnn(toTokens(textAnn.text, tokenOffsetAnn.tokens)).tokens.map(Tuple2(_, 1))
      }).
      reduceByKey(_ + _).
      repartition(1).
      mapPartitions(x => {
        var c = 0
        x.map(t => {
          val tup = (c, t)
          c += 1
          tup
        })        
      })

    val tokensCount = allTokens.count.toInt
      
    val tokenInst2basic = rdd.
      flatMap { case (documentID, annotations) => {
    	// create (token, (documentID, sentNum, pos))
        val l = new ArrayBuffer[(String,(Long,Int,Int))]()
        val textAnn = annotations(textAnnID).asInstanceOf[TextAnn]
        val tokenOffsetAnn = annotations(tokenOffsetAnnID).asInstanceOf[TokenOffsetAnn]      
        val tokens = toTokens(textAnn, tokenOffsetAnn).tokens
        val sentenceTokenOffsetAnn = annotations(sentenceTokenOffsetAnnID).asInstanceOf[SentenceTokenOffsetAnn]
        for (i <- 0 until sentenceTokenOffsetAnn.sents.length) {
          val s = sentenceTokenOffsetAnn.sents(i)
          for (j <- s.f until s.t)
            l += Tuple2(tokens(j), Tuple3(documentID, i, j))
        }
        l
      }}.
      join(
        allTokens.map { case (tokenID, (token, frequency)) => (token, (tokenID, frequency))}).
      map { case (token, ((documentID, sentNum, pos), (tokenID, frequency))) =>
        // create
        (documentID, sentNum, pos, tokenID, token, frequency)
      }
    
    val rows = tokenInst2basic.
    map { case (documentID, sentNum, _, tokenID, _, frequency) => 
      ((documentID, sentNum), (tokenID, 1.0 / frequency))
    }.
    groupByKey.
    sortByKey().
    map { case ((documentID, sentNum), it) =>
      val dedup = it.groupBy(_._1).map(x => (x._1, x._2.size * x._2.head._2))
      //val r = it.reduce((x,y) => (x._1, x._2 + y._2))
      ((documentID, sentNum), Vectors.sparse(tokensCount, dedup.toSeq))
    }
    
    val mat: RowMatrix = new RowMatrix(rows.map(_._2).persist)

    // Compute 20 largest singular values and corresponding singular vectors
    val svd = mat.computeSVD(20, computeU = true)
 
    // Write results to hdfs
    // right-singular vectors
    val V = svd.V.toArray.grouped(svd.V.numRows).toList.transpose
    sc.makeRDD(V, 1).zipWithIndex()
      .map(line => line._2 + "\t" + line._1.mkString("\t")) // make tsv line starting with column index
      .saveAsTextFile(outDir + "/svdTokenVectors")
 
    // left-singular vectors
    svd.U.rows.map(row => row.toArray).zip(rows.map(_._1))
      .map(line => line._2._1 + "\t" + line._2._2 + "\t" + line._1.mkString("\t")) // make tsv line starting with row index
      .saveAsTextFile(outDir + "/svdSentenceVectors")
 
    sc.makeRDD(svd.s.toArray, 1)
      .saveAsTextFile(outDir + "/svdValues")   
    
    
    //allTokens.map(tsv(_)).saveAsTextFile(outDir + "/token2name")
      
    //allTokens
  }
  
    // finds Source
  def run(outDir:String, rdd:RDD[(Long,Array[Any])])(implicit sc:SparkContext):Unit = {
    val c0 = firstColumnOfType(rdd, classOf[TextAnn])
    val c1 = firstColumnOfType(rdd, classOf[TokenOffsetAnn])
    val c2 = firstColumnOfType(rdd, classOf[SentenceTokenOffsetAnn])
    run(outDir, rdd, c0, c1, c2)
  }

}