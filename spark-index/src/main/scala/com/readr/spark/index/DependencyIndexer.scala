package com.readr.spark.index

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

import scala.collection.mutable._
import scala.collection.JavaConversions._

import com.readr.model._
import com.readr.model.annotation._
import com.readr.spark.util.Utils._


object DependencyIndexer {
  
  def run(outDir:String, rdd:RDD[(Long,Array[Any])], sentenceDependencyAnnID:Int, sentenceTokenOffsetAnnID:Int)
  	(implicit sc:SparkContext):Unit = {
    
    val flatDeps = rdd.flatMap(x => {
        val l = new ArrayBuffer[(String,(Long,Int,Int,Int,Int))]()
        val id = x._1
        val sentenceDependencyAnn = x._2(sentenceDependencyAnnID).asInstanceOf[SentenceDependencyAnn]
        val sentenceTokenOffsetAnn = x._2(sentenceTokenOffsetAnnID).asInstanceOf[SentenceTokenOffsetAnn]
        
        for (sentNum <- 0 until sentenceDependencyAnn.sents.size) {
          val senOff = sentenceTokenOffsetAnn.sents(sentNum)
          val sen = sentenceDependencyAnn.sents(sentNum)
          for (d <- sen)
            l += Tuple2(d.name, Tuple5(id, sentNum, d.from, d.to, senOff.f))
        }
        l
      }
    )

    val allDependencyNames = flatDeps.map(x => x._1).distinct
    
    val dependency2name = allDependencyNames.repartition(1).mapPartitions(x => { 
      var c = 0
      x.map(t => {
        val tup = (c, t)
        c += 1
        tup
      })
    })
    
    val joined = flatDeps.join(dependency2name.map(_.swap))
    
    val dependencyInst2basic = joined.map(x => {
      val dependencyID = x._2._2
      val documentID = x._2._1._1
//      val sentNum = x._2._1._2
      val senOff = x._2._1._5
      val tokenFrom = x._2._1._3 + senOff
      val tokenEnd = x._2._1._4 + senOff
      (dependencyID, documentID, /*sentNum,*/ tokenFrom, tokenEnd)
    })

//    // without dependencyID ...
//    val sentenceDependency = flatDeps.map(x => {
//      val name = x._1
//      val documentID = x._2._1
//      val sentNum = x._2._2
//      //val senOff = x._2._5
//      val tokenFrom = x._2._3 //+ senOff
//      val tokenEnd = x._2._4 //+ senOff
//      ((documentID, sentNum), name + ":" + tokenFrom + ":" + tokenEnd)
//    }).groupBy(_._1).map(x => (x._1._1, x._1._2, x._2.map(x=>x._2).mkString(" ") ))

    val sentenceDependency = rdd.flatMap(x => {
        val l = new ArrayBuffer[(Long,Int,String)]()
        val id = x._1
        val sentenceDependencyAnn = x._2(sentenceDependencyAnnID).asInstanceOf[SentenceDependencyAnn]
        val sentenceTokenOffsetAnn = x._2(sentenceTokenOffsetAnnID).asInstanceOf[SentenceTokenOffsetAnn]
        
        for (sentNum <- 0 until sentenceDependencyAnn.sents.size) {
          val senOff = sentenceTokenOffsetAnn.sents(sentNum)
          val sen = sentenceDependencyAnn.sents(sentNum)          
          val deps = sen.map(d => d.name + ":" + d.from + ":" + d.to).mkString(" ")          
          l += Tuple3(id, sentNum, deps)
        }
        l
    })
    
//    if we need dependencyID ...
//    val sentenceDependency = joined.map(x => {
//      val name = x._1
//      val dependencyID = x._2._2
//      val documentID = x._2._1._1
//      val sentNum = x._2._1._2
//      //val senOff = x._2._1._5
//      val tokenFrom = x._2._1._3
//      val tokenEnd = x._2._1._4
//      ((documentID, sentNum), dependencyID + ":" + tokenFrom + ":" + tokenEnd)
//    }).groupBy(_._1).map(x => (x._1._1, x._1._2, x._2.map(x=>x._2).mkString(" ") ))
    
    dependency2name.map(tsv(_)).saveAsTextFile(outDir + "/dependency2name")
    dependencyInst2basic.map(tsv(_)).saveAsTextFile(outDir + "/dependencyInst2basic")
    sentenceDependency.map(tsv(_)).saveAsTextFile(outDir + "/sentenceDependency")
  }
  
  
  // finds Source
  def run(outDir:String, rdd:RDD[(Long,Array[Any])])(implicit sc:SparkContext):Unit = {
    val c0 = firstColumnOfType(rdd, classOf[SentenceDependencyAnn])
    val c1 = firstColumnOfType(rdd, classOf[SentenceTokenOffsetAnn])
    run(outDir, rdd, c0, c1)
  }  
}
