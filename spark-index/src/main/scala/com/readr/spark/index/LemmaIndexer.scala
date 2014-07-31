package com.readr.spark.index

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

import scala.collection.mutable._
import scala.collection.JavaConversions._

import com.readr.model._
import com.readr.model.annotation._
import com.readr.spark.util.Utils._


object LemmaIndexer {
  
  def run(outDir:String, rdd:RDD[(Long,Array[Any])], lemmaAnnID:Int)
  	(implicit sc:SparkContext):Unit = {
    
//    val pos = rdd.flatMap(x => {
//        val l = new ArrayBuffer[(Long,Int,String)]()
//        val id = x._1
//        val posAnn = x._2(posAnnID).asInstanceOf[POSAnn]
//        for (i <- 0 until posAnn.pos.size)
//          l += Tuple3(id, i, posAnn.pos(i))
//        l
//      }
//    )
    
    val allLemma = rdd.flatMap(x => {
        val lemmaAnn = x._2(lemmaAnnID).asInstanceOf[LemmaAnn]
        lemmaAnn.lemmas
      }
    ).distinct

    val lemma2name = allLemma.repartition(1).mapPartitions(x => { 
      var c = 0
      x.map(t => {
        val tup = (c, t)
        c += 1
        tup
      })
    })
    
    val lemmaInst2basic = rdd.flatMap(x => {
        val l = new ArrayBuffer[(String,(Long,Int))]()
        val id = x._1
        val lemmaAnn = x._2(lemmaAnnID).asInstanceOf[LemmaAnn]
        for (i <- 0 until lemmaAnn.lemmas.size)
          l += Tuple2(lemmaAnn.lemmas(i), Tuple2(id, i))
        l      
    }).join(lemma2name.map(_.swap)).map(x => {
      val lemmaID = x._2._2
      val documentID = x._2._1._1
      val pos = x._2._1._2
      (documentID, pos, lemmaID)
    })
    // TODO: sortByKey, put all in pair tuple2 as key and create comparator on keys?

    lemma2name.map(tsv(_)).saveAsTextFile(outDir + "/lemma")
    lemmaInst2basic.map(tsv(_)).saveAsTextFile(outDir + "/lemmaInst")
  }
  
  
  // finds Source
  def run(outDir:String, rdd:RDD[(Long,Array[Any])])(implicit sc:SparkContext):Unit = {
    val c0 = firstColumnOfType(rdd, classOf[LemmaAnn])
    run(outDir, rdd, c0)
  }

}
