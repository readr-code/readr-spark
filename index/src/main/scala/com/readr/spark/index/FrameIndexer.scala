package com.readr.spark.index

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

//import scala.collection.mutable._
import scala.collection.JavaConversions._

import com.readr.model._
import com.readr.model.annotation._
import com.readr.model.frame._
import com.readr.spark.util.Utils._

// creates indices for local (document-level) clusters and distant (cross-document) clusters


object FrameIndexer {

  def run(outDir:String, rddFrame:RDD[(Long,Frame,Seq[FrameMatchFeature])], rdd:RDD[(Long,Array[Any])])(implicit sc:SparkContext):Unit = {
    
    // NER outputs MentionAnn, FrameAnns...
    
    // stanfordNER:
    //   1 arg: head
    //  frame with one arg
    
    
    
    // frame: 
    //    name: mentionSpan
    //    1 arg: head
    //    pattern: head2MentionSpan(head, "   ")
    // populate frameMatchFeatures here
    
    
    // frame: frameID, name, description, examples, typ
    // frameArg: frameID, position, name, description, required, valueFrameID, typeFrameID
         
    // frameRelation:frameRelationID, parentFrameID, childFrameID, frameRelationType
    // frameArgRelation:frameRelationID,parentFrameArgID,childFrameArgID

    // frameMatchFeature..
    // instanceID, frameID, truth, priority
    // instanceID, frameArgID, documentID, pos
    
    
    
    val all = rddFrame.repartition(1).mapPartitions(x => {
      var instanceID = -1
      var frameArgID = -1
      x.map(t => {
        val frameID = t._1
        val f = t._2
        val s = t._3
        
        val frame = Tuple5(frameID, f.name, f.description, f.examples, f.typ.id)

        val frameArg = f.args.map(x =>
          Tuple5(frameID, x.argNum, x.name, x.description, if (x.required) 1 else 0)
        )
        
        val frameValence = f.valences.map(x => Tuple3(frameID, x.inheritedFrameID, x.text))
      
        //val (frameMatchFeature:Tuple4[Int,Long,Boolean,Int], dframeMatchFeatureArg:Seq[Tuple4[Int,Int,Int,Int]]) 
        val tt = s.map(x => {
          instanceID += 1
          val fmf = Tuple4(instanceID, frameID, if (x.truth) 1 else 0, x.priority)
        
          val fmfa = for (i <- 0 until x.args.size) yield {
            val argNum = frameArg(i)._2
            Tuple4(instanceID, argNum, x.args(i).documentID, x.args(i).pos)
          }
          (fmf, fmfa)
        })
        
        val frameCounts = Tuple9(frameID, tt.size, tt.size, 0, 0, 0, 0, 0, 0)
        
        (frame, frameArg, frameValence, frameCounts, tt)
      })
    })
    
    val frame = all.map(x => x._1)
    val frameArg = all.flatMap(x => x._2)
    val frameValence = all.flatMap(x => x._3)
    val frameCounts = all.map(x => x._4)
    val frameMatchFeature = all.flatMap(x => x._5).map(x => x._1)
    val frameMatchFeatureArg = all.flatMap(x => x._5).flatMap(x => x._2)
    
    frame.map(tsv(_)).saveAsTextFile(outDir + "/frame")
    frameArg.map(tsv(_)).saveAsTextFile(outDir + "/frameArg")
    frameValence.map(tsv(_)).saveAsTextFile(outDir + "/frameValence")
    frameCounts.map(tsv(_)).saveAsTextFile(outDir + "/frameCounts")
    frameMatchFeature.map(tsv(_)).saveAsTextFile(outDir + "/frameMatchFeature")
    frameMatchFeatureArg.map(tsv(_)).saveAsTextFile(outDir + "/frameMatchFeatureArg")
  }  

}
