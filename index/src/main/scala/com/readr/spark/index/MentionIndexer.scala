package com.readr.spark.index

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

import scala.collection.mutable._
import scala.collection.JavaConversions._

import com.readr.model._
import com.readr.model.annotation._
import com.readr.spark.util.Utils._


object MentionIndexer {

  // assume RDD format (id, NERAnn)
  def run(outDir:String, rdd:RDD[(Long,Array[Any])], mentionAnnID:Int)
  	(implicit sc:SparkContext):Unit = {
    
    // documentID, mentionNum, head, start, end, mentionTyp, number, gender, animacy    
    val mention = rdd.flatMap(x => {
        val l = new ArrayBuffer[(Long,Int,Int,Int,Int,Int,Int,Int,Int)]()
        val id = x._1
        val mentionAnn = x._2(mentionAnnID).asInstanceOf[MentionAnn]
        for (m <- mentionAnn.mentions)
          l += Tuple9(id, m.mentionNum, m.head, m.tokenOffsets.f, m.tokenOffsets.t, 
              m.mentionTyp, m.number, m.gender, m.animacy)
        l
      }
    )
        
    mention.map(tsv(_)).saveAsTextFile(outDir + "/mention")
  }  
  
  // finds Source
  def run(outDir:String, rdd:RDD[(Long,Array[Any])])(implicit sc:SparkContext):Unit = {
    val c0 = firstColumnOfType(rdd, classOf[MentionAnn])
    run(outDir, rdd, c0)
  }

}
