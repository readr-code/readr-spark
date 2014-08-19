//package com.readr.spark.index
//
//import org.apache.spark.SparkContext
//import org.apache.spark.SparkContext._
//import org.apache.spark.rdd._
//
//import scala.collection.mutable._
//import scala.collection.JavaConversions._
//
//import com.readr.model._
//import com.readr.model.annotation._
//import com.readr.spark.util.Utils._
//
//
//object MentionIndexer {
//
//  // better: no global mentionID,
//  // the ID is (documentID, mentionID), where mentionID is local to the document
//  // or (documentID, head) ??
///*
// * Generates 2 files:
// * mention2basic : { mentionID, sentenceID, head, start, end, repMentionID, mentionType, number, gender, animacity }
// * repProperMention2basic : { mentionID, name }
// * properMentionSpans : { sentenceID, head, mentionSpan }
// */
//
////DEFINE createGloballyUniqueIDs(A) RETURNS C {
////	B = GROUP $A ALL;
////	$C = FOREACH B GENERATE flatten(com.readr.process.module.coreference.GloballyUniqueIDs($1)) PARALLEL 1;
////};
////
////-- read sequence file
////protoCoref = loadProtobufCoreference('$dir/doc.Coref');
////protoConst = loadProtobufConstituent('$dir/doc.Constituent');
////
////-- assign a (globally) unique ID to each chain and each mention
////A = createGloballyUniqueIDs(protoCoref);
////B = FOREACH A GENERATE documentID, value.chains;
////
////-- assign (globally) unique sentenceIDs to const
////C = FOREACH protoConst GENERATE documentID, FLATTEN(value.sentences);
////D = prependGlobalRowNumber(C);
////E = FOREACH D GENERATE $0 AS sentenceID, documentID, $2 AS constituents;
////F = GROUP E BY documentID;
////--F : {documentID, {(sentenceID, documentID, constitutents)}}
////
////-- now join and compute mentions
////G = JOIN B BY documentID, F BY $0; -- USING 'replicated';
////H = FOREACH G GENERATE documentID, FLATTEN(com.readr.hadoop.module.coreference.GetMentions($1, $3));
////I = FOREACH H GENERATE $1, $2, $3, $4, $5, $6, $7, $8, $9, $10;
////--J = FOREACH H GENERATE $2, $3, $11, $12;
////J = FILTER H BY $12 ==1 AND com.readr.hadoop.module.coreference.ShortString($11);
////K = FOREACH J GENERATE $2, $3, $11;
////-- order them?
////
////-- store
////STORE I INTO '$dir/db/mention2basic';
////STORE K INTO '$dir/db/properMentionSpans';
//  
//  // assume RDD format (id, NERAnn)
//  def run(outDir:String, rdd:RDD[(Long,Array[Any])], mentionAnnID:Int)
//  	(implicit sc:SparkContext):Unit = {
//    
//    // documentID, head, start, end, span, corefClusterID, isProper, isRepresentative 
//    val mention = rdd.flatMap(x => {
//        val l = new ArrayBuffer[(Long,Int,Int,Int,String,Int,Int,Int)]()
//        val id = x._1
//        val mentionAnn = x._2(mentionAnnID).asInstanceOf[MentionAnn]
//        for (m <- mentionAnn.mentions) {
//          val span = if (m.mentionSpan.length > 100) m.mentionSpan.substring(0, 97) + "..." else m.mentionSpan
//          val isProper = if (m.isProper) 1 else 0
//          val isRepresentative = if (m.isRepresentative) 1 else 0
//          
//          l += Tuple8(id, m.head, m.tokenOffsets.f, m.tokenOffsets.t, span, m.corefClusterID, isProper, isRepresentative)
//        } 
//        l
//      }
//    )
//    
//    val repSpanCounts = rdd.flatMap(x => {
//        val l = new ArrayBuffer[(String,Int)]
//        val mentionAnn = x._2(mentionAnnID).asInstanceOf[MentionAnn]
//        // count mentions in clusters
//        val counts = Map[Int,Int]()
//        for (m <- mentionAnn.mentions) {
//          val c = counts.getOrElse(m.corefClusterID, 0)
//          counts.put(m.corefClusterID, c+1)
//        }
//        for (m <- mentionAnn.mentions)
//          if (m.isRepresentative)
//            l += Tuple2(m.mentionSpan, counts(m.corefClusterID))
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
////    val properMentions = rdd.flatMap(x => {
////        val l = new ArrayBuffer[(Long,Int,String,Int,Int)]()
////        val id = x._1
////        val mentionAnn = x._2(mentionAnnID).asInstanceOf[MentionAnn]
////        for (m <- mentionAnn.mentions)
////          if (m.proper)
////          l += Tuple5(id, m.head, m.mentionSpan, m.tokenOffsets.f, m.tokenOffsets.t)
////        l
////      }
////    )
////    mentions.map(tsv(_)).saveAsTextFile(outDir + "/mentions")
//    
///*
// * Generates 2 files:
// * mention2cluster : { sentenceID, head, distantClusterID }
// * distantCluster2basic : { distantClusterID, name, count }
// */
//
////A2 = LOAD '$dir/db/properMentionSpans' AS (sentenceID:int, head:int, mentionSpan:charArray);
////B2 = GROUP A2 BY mentionSpan;
////C2 = prependGlobalRowNumber(B2);
////
////D2 = FOREACH C2 GENERATE $0 AS distantClusterID, flatten($2);
////E2 = FOREACH D2 GENERATE $1 AS sentenceID, $2 AS head, $0 AS distantClusterID;
////F2 = FOREACH C2 GENERATE $0 AS distantClusterID, $1 AS name, COUNT($2) AS count; 
////
////G2 = ORDER E2 BY sentenceID, head, distantClusterID;
////H2 = DISTINCT G2;
////I2 = ORDER F2 BY distantClusterID, name;
////
////-- store
////STORE H2 INTO '$dir/db/mention2cluster';
////STORE I2 INTO '$dir/db/distantCluster2basic';    
//
////    // distantClusters: { distantClusterID, name, count }
////    val distantCluster = mention.map(x => (x._3, 1)).reduceByKey((a,b)=> a+b).repartition(1).mapPartitions(x => { 
////      var c = 0
////      x.map(t => {
////        val tup = (c, t._1, t._2)
////        c += 1
////        tup
////      })
////    })
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
//    
//    mention.map(tsv(_)).saveAsTextFile(outDir + "/mention")
//    distantCluster.map(tsv(_)).saveAsTextFile(outDir + "/distantCluster")
//    corefCluster.map(tsv(_)).saveAsTextFile(outDir + "/corefCluster")
//    //mention2cluster.map(tsv(_)).saveAsTextFile(outDir + "/mention2cluster")
//  }  
//  
//  // finds Source
//  def run(outDir:String, rdd:RDD[(Long,Array[Any])])(implicit sc:SparkContext):Unit = {
//    val c0 = firstColumnOfType(rdd, classOf[MentionAnn])
//    run(outDir, rdd, c0)
//  }
//
//}
