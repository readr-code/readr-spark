package com.readr.spark.index

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

import scala.collection.mutable._
import scala.collection.JavaConversions._

import com.readr.model._
import com.readr.model.annotation._
import com.readr.spark.util.Utils._


object POSIndexer {
  
  def run(outDir:String, rdd:RDD[(Long,Array[Any])], posAnnID:Int)
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
    
    val allPos = rdd.flatMap(x => {
        val posAnn = x._2(posAnnID).asInstanceOf[POSAnn]
        posAnn.pos
      }
    ).distinct

    val pos2name = allPos.repartition(1).mapPartitions(x => { 
      var c = 0
      x.map(t => {
        val tup = (c, t)
        c += 1
        tup
      })
    })
    
    val posInst2basic = rdd.flatMap(x => {
        val l = new ArrayBuffer[(String,(Long,Int))]()
        val id = x._1
        val posAnn = x._2(posAnnID).asInstanceOf[POSAnn]
        for (i <- 0 until posAnn.pos.size)
          l += Tuple2(posAnn.pos(i), Tuple2(id, i))
        l      
    }).join(pos2name.map(_.swap)).map(x => {
      val partOfSpeechID = x._2._2
      val documentID = x._2._1._1
      val pos = x._2._1._2
      (documentID, pos, partOfSpeechID)
    })
    // TODO: sortByKey, put all in pair tuple2 as key and create comparator on keys?

    pos2name.map(tsv(_)).saveAsTextFile(outDir + "/partOfSpeech")
    posInst2basic.map(tsv(_)).saveAsTextFile(outDir + "/partOfSpeechInst")
  }
  
  
  // finds Source
  def run(outDir:String, rdd:RDD[(Long,Array[Any])])(implicit sc:SparkContext):Unit = {
    val c0 = firstColumnOfType(rdd, classOf[POSAnn])
    run(outDir, rdd, c0)
  }

}
