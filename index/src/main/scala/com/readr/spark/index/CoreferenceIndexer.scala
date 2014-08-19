package com.readr.spark.index

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

import scala.collection.mutable._
import scala.collection.JavaConversions._

import com.readr.model._
import com.readr.model.annotation._
import com.readr.spark.util.Utils._

// creates indices for local (document-level) clusters and distant (cross-document) clusters


object CoreferenceIndexer {

  //DistantFrameIndexer
  //   frame
  
/*
 * mention2basic : { mentionID, sentenceID, head, start, end, repMentionID, mentionType, number, gender, animacity }
 */
  
  // mention2coref: documentID, mentionNum, chainNum    ///////, distantClusterID (=frameID?)
  // corefCluster: documentID, chainNum, mentionSpan       ==> distant

  
  // distantCluster: distantClusterID, name, count
  /////// distantCluster: distant
  // mention2distant: documentID, mentionNum, distantClusterID (=frameID)
  
  def run(outDir:String, rdd:RDD[(Long,Array[Any])], textAnnID:Int, tokenOffsetAnnID:Int, 
      mentionAnnID:Int, coreferenceAnnID:Int)
  	(implicit sc:SparkContext):Unit = {
    
    
    // documentID, head, start, end, span, corefClusterID, isProper, isRepresentative 
    val mention2cluster = rdd.flatMap(x => {
        val l = new ArrayBuffer[(Long,Int,Int)]()
        val id = x._1
        //val textAnn = x._2(textAnnID).asInstanceOf[TextAnn]
        //val tokenOffsetAnn = x._2(tokenOffsetAnnID).asInstanceOf[TokenOffsetAnn]
        //val mentionAnn = x._2(mentionAnnID).asInstanceOf[MentionAnn]
        val coreferenceAnn = x._2(coreferenceAnnID).asInstanceOf[CoreferenceAnn]
        for (c <- coreferenceAnn.chains)
          for (mentionNum <- c.mentionNums)
            l += Tuple3(id, mentionNum, c.chainNum)
        l
    })
    
    val corefCluster = rdd.flatMap(x => {
        val l = new ArrayBuffer[(Long,Int,String)]()
        val id = x._1
        val textAnn = x._2(textAnnID).asInstanceOf[TextAnn]
        val tokenOffsetAnn = x._2(tokenOffsetAnnID).asInstanceOf[TokenOffsetAnn]
        val mentionAnn = x._2(mentionAnnID).asInstanceOf[MentionAnn]
        val coreferenceAnn = x._2(coreferenceAnnID).asInstanceOf[CoreferenceAnn]
        for (c <- coreferenceAnn.chains) {
          val m = mentionAnn.mentions(c.representativeMentionNum)
          
          val span = textAnn.text.substring(tokenOffsetAnn.tokens(m.tokenOffsets.f).f, 
              tokenOffsetAnn.tokens(m.tokenOffsets.t - 1).t)
          
          val shortSpan = if (span.length > 100) span.substring(0, 100) else span
          
          l += Tuple3(id, c.chainNum, shortSpan)
        }
        l
      }
    )
    
//    val repSpanCounts = rdd.flatMap(x => {
//        val l = new ArrayBuffer[(String,Int)]()
//        val textAnn = x._2(textAnnID).asInstanceOf[TextAnn]
//        val tokenOffsetAnn = x._2(tokenOffsetAnnID).asInstanceOf[TokenOffsetAnn]
//        val mentionAnn = x._2(mentionAnnID).asInstanceOf[MentionAnn]
//        val coreferenceAnn = x._2(coreferenceAnnID).asInstanceOf[CoreferenceAnn]
//        // count mentions in clusters
//        val counts = Map[Int,Int]()
//        for (c <- coreferenceAnn.chains) {
//          val m = mentionAnn.mentions(c.representativeMentionNum)
//          
//          val span = textAnn.text.substring(tokenOffsetAnn.tokens(m.tokenOffsets.f).f, 
//              tokenOffsetAnn.tokens(m.tokenOffsets.t - 1).t)
//          
//          val shortSpan = if (span.length > 100) span.substring(0, 100) else span
//          
//          l += Tuple2(shortSpan, c.mentionNums.size)
////        for (m <- mentionAnn.mentions) {
////          val c = counts.getOrElse(m.corefClusterID, 0)
////          counts.put(m.corefClusterID, c+1)
////        }
////        for (m <- mentionAnn.mentions)
////          if (m.isRepresentative)
////            l += Tuple2(m.mentionSpan, counts(m.corefClusterID))
//        }
//        l
//      }
//    ).reduceByKey(_ + _)
//    
//    
//    val distantCluster = repSpanCounts.repartition(1).mapPartitions(x => { 
//      var c = 0
//      x.map(t => {
//        val tup = (c, t._1, t._2)
//        c += 1
//        tup
//      })
//    })
//
//    val corefCluster = rdd.flatMap(x => {
//        val l = new ArrayBuffer[(String,(Long,Int))]
//        val documentID = x._1
//        val mentionAnn = x._2(mentionAnnID).asInstanceOf[MentionAnn]
//        for (m <- mentionAnn.mentions)
//          if (m.isRepresentative)
//            l += Tuple2(m.mentionSpan, Tuple2(documentID, m.corefClusterID))
//        l
//      }
//    ).join(distantCluster.map(x => Tuple2(x._2, x._1))).map(x => {
//      val span = x._1
//      val documentID = x._2._1._1
//      val corefClusterID = x._2._1._2
//      val distantClusterID = x._2._2
//      Tuple3(documentID, corefClusterID, distantClusterID)
//    })//.distinct
//    // note: distinct necessary, because we currently have mentions with same documentID and head
//    
//
//
////    // mention2cluster
////    val mention2cluster = mention.map(x => Tuple2(x._3, Tuple2(x._1, x._2))).join(distantCluster.map(x => Tuple2(x._2, x._1))).map(x => {
////      val documentID = x._2._1._1
////      val head = x._2._1._2
////      val distantClusterID = x._2._2
////      Tuple3(documentID, head, distantClusterID)
////    }).distinct
////    // note: distinct necessary, because we currently have mentions with same documentID and head
    
    //mention.map(tsv(_)).saveAsTextFile(outDir + "/mention")
    //distantCluster.map(tsv(_)).saveAsTextFile(outDir + "/distantCluster")
    mention2cluster.map(tsv(_)).saveAsTextFile(outDir + "/mention2cluster")
    corefCluster.map(tsv(_)).saveAsTextFile(outDir + "/corefCluster")
    //mention2cluster.map(tsv(_)).saveAsTextFile(outDir + "/mention2cluster")
  }  
  
  // finds Source
//  def run(outDir:String, rdd:RDD[(Long,Array[Any])])(implicit sc:SparkContext):Unit = {
//    val c0 = firstColumnOfType(rdd, classOf[MentionAnn])
//    val c1 = firstColumnOfType(rdd, classOf[CoreferenceAnn])
//    run(outDir, rdd, c0)
//  }

}
