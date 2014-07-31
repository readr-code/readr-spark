package com.readr.spark.index

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

import scala.util.control.Breaks._
import scala.collection.mutable._
import scala.collection.JavaConversions._

import com.readr.model._
import com.readr.model.annotation._
import com.readr.spark.util.Utils._

object PosPairFeatureIndexer {

// patternBinaryInst:  { documentID, pos1, pos2, patternID }
// patternBinary: { patternID, pattern }
// patternBinaryCount: { patternID, count }
  
  def run(outDir:String, rdd:RDD[(Long,Array[Any])], posPairFeatureAnnID:Int, sentenceTokenOffsetAnnID:Int)
  	(implicit sc:SparkContext):Unit = {
    
    val posPairFeatures = rdd.flatMap(x => {
        val l = new ArrayBuffer[(String,(Long,Int,Int))]()
        val id = x._1
        val posPairFeatureAnn = x._2(posPairFeatureAnnID).asInstanceOf[PosPairFeatureAnn]
        val sentenceTokenOffsetAnn = x._2(sentenceTokenOffsetAnnID).asInstanceOf[SentenceTokenOffsetAnn]

        for (sentNum <- 0 until posPairFeatureAnn.sents.length) {
          val senOff = sentenceTokenOffsetAnn.sents(sentNum).f
          for (feature <- posPairFeatureAnn.sents(sentNum))
            l += Tuple2(feature.feature, Tuple3(id, feature.pos1 + senOff, feature.pos2 + senOff))
        }
        l
      }
    )
    
    // (feature, [(documentID, pos1, pos2)), ...])
    val grouped = posPairFeatures.groupBy(_._1)

    val allPatterns = grouped.map(x => x._1) //posPairFeatures.map(x => x._1).distinct
    
    val pattern2name = allPatterns.repartition(1).mapPartitions(x => { 
      var c = 0
      x.map(t => {
        val tup = (c, t)
        c += 1
        tup
      })
    })
    val posPairFeature = pattern2name.sortByKey()
    
    // (feature, (
    val joined = grouped.join(pattern2name.map(_.swap))
    //val joined = posPairFeatures.join(pattern2name.map(_.swap))

    // (String, ( Iterable , Int ))
    val posPairFeatureInst = joined.flatMap(x => {
      val arr = new ArrayBuffer[(Long,Int,Int,Int)]
      val patternID = x._2._2
      val it = x._2._1.iterator
      while (it.hasNext) {
        val t = it.next
        val documentID = t._2._1
        val pos1 = t._2._2
        val pos2 = t._2._3
        arr += new Tuple4(documentID, pos1, pos2, patternID)
      }
      arr
    }).map(x => Tuple2(x, 0)).sortByKey().map(x => x._1)
    // order by columns in order
        
    val ps = posPairFeatureInst.map(x => Tuple2(x, 0)).sortByKey().map(x => x._1)
    
    val posPairFeatureCount = joined.map(x => {
      val patternID = x._2._2
      val it = x._2._1
      Tuple2(patternID, it.size)
    }).sortByKey()
          
    posPairFeature.map(tsv(_)).saveAsTextFile(outDir + "/posPairFeature")
    posPairFeatureInst.map(tsv(_)).saveAsTextFile(outDir + "/posPairFeatureInst")
    posPairFeatureCount.map(tsv(_)).saveAsTextFile(outDir + "/posPairFeatureCount")    
  }  

  def run(outDir:String, rdd:RDD[(Long,Array[Any])])(implicit sc:SparkContext):Unit = {
    val c0 = firstColumnOfType(rdd, classOf[PosPairFeatureAnn])
    val c1 = firstColumnOfType(rdd, classOf[SentenceTokenOffsetAnn])
    run(outDir, rdd, c0, c1)
  }  
}
