//package com.readr.spark.other
//
//import org.apache.spark.SparkContext
//import org.apache.spark.SparkContext._
//import scala.collection.mutable.HashSet
//
//import scala.collection.JavaConversions._
//
//import com.readr.model._
//import com.readr.model.annotation._
//
//import com.readr.spark.util.Annotator
//
//import scala.collection.mutable._
//import java.util.Properties
//import java.util.Vector
//
//object SimpleMentionExtractor {
//  
//}
//
//class SimpleMentionExtractor extends Annotator(
//      generates = Array(classOf[SentenceMentionAnn], classOf[MentionAnn]),
//      requires = Array(classOf[ConstituentAnn], classOf[CorefAnn], classOf[SentenceTokenOffsetAnn])) {
//  
//  val properties = new Properties()
//  
//  override def annotate(ins:Any*):Array[Any] = {
//    val out = run(ins(0).asInstanceOf[ConstituentAnn],
//        ins(1).asInstanceOf[CorefAnn], ins(2).asInstanceOf[SentenceTokenOffsetAnn])     
//    Array(out._1, out._2)
//  }
//  
//  def run(ca:ConstituentAnn, cra:CorefAnn, stoa:SentenceTokenOffsetAnn):(SentenceMentionAnn,MentionAnn) = {
//    
//	// 1. identify relevant constituents
//    //    one bool for each token, indicating if it is a leaf of type DT/NNP/NNPS/IN
//	val tokenInConst = new ArrayBuffer[Vector[java.lang.Boolean]](ca.sents.length)
//	for (sca <- ca.sents) {
//	  val tags = new Vector[java.lang.Boolean]()
//			
//	  for (con <- sca) {
//	    // only look at leaf nodes
//		if (con.offsets.t - con.offsets.f == 1) {
//				
//		  val tag = con.name
//		  if (tag.equals("DT") || tag.equals("NNP") || tag.equals("NNPS") || tag.equals("IN")) {
//		    val pos = con.offsets.f
//		    //tags.ensureCapacity(pos+1);
//		    if (pos > tags.size-1) tags.setSize(pos+1)
//			  //tags.setSize(newSize)
//			  tags.set(pos, true)
//		  }
//		}
//	  }
//	  tokenInConst += tags
//	}
//
//	// 2. check mentions identified by coref system, that are in these
//	//    constituents
//	val sentNum2mentions = 
//	  HashMap[Int,ArrayBuffer[SentenceMention]]()
//	
//	// mentionID will be unique only to document
//	//var mentionID = 0
//	
//	//TODO: throwing all sentences together: not sure if this is desired.
//	val lps = ArrayBuffer[Array[SentenceMention]]()
//
//	// for each coref chain ...
//	for (cc <- cra.chains) {
//
//	  val l = new ArrayBuffer[FeaturizedMention]()
//			
//	  // for each mention in coref chain ...
//	  for (cm <- cc.mentions) {
//				
//		// check if all tokens of span match one of the above constituent types
//		val tags = tokenInConst(cm.sentNum-1)
//		var satisfied = true
//		for (i <- cm.startIndex - 1 until cm.endIndex -1) //.positionFrom until cm.positionTo)
//		
//		//for (i <- cm.positionFrom until cm.positionTo)
//		  if (i >= tags.size || tags.get(i) == null || !tags.get(i)) satisfied = false
//				
//		// want to drop determiner at beginning and possessive 's at end
//		// -> that helps with later disambiguation
//		var name = cm.mentionSpan
//		if (name.startsWith("the ")) name = name.substring(4)
//		if (cm.startIndex-1 == 0 && name.startsWith("The ")) name = name.substring(4)
//		//if (cm.positionFrom == 0 && name.startsWith("The ")) name = name.substring(4)
//		if (name.endsWith(" 's")) name = name.substring(0, name.length-3)
//				
//		val fm = FeaturizedMention(
//		  name = name,
//		  great = satisfied,
//		  proper = cm.mentionTyp.endsWith("PROPER"),
//		  startPos = cm.startIndex - 1,
//		  endPos = cm.endIndex - 1,
//		  headPos = cm.headIndex - 1,
//		  sentNum = cm.sentNum - 1,
//		  positionFrom = cm.positionFrom,
//		  positionTo = cm.positionTo,
//		  corefClusterID = cm.corefClusterID,
//		  mentionSpan = name,
//		  mentionType = cm.mentionTyp,
//		  number = cm.number,
//		  gender = cm.gender,
//		  animacy = cm.animacy
//		)
//		l += fm
//	  }
//
//			
//	  // process this chain
//		
//	  // determine representative featurized mention
//	  var rep:FeaturizedMention = null
//	  var repMentionID = -1
//	  for (j <- 0 until l.size) {
//		val cur = l(j) //.copy(mentionID = mentionID + j)
//	    l(j) = cur
//		if (rep == null ||
//		  (!rep.proper && cur.proper) ||
//		  (!rep.great && cur.great)) {
//			// find shortest mention with same head
//			//if (headPos == repHeadPos &&
//			//		(startPos > repStartPos || endPos < repEndPos)) {
//			// if it is not proper, ignore
//			//if (!representativeMention[10].equals("PROPER")) return;
//			rep = cur
//			repMentionID = j
//		}
//	  }
//	  //mentionID = mentionID + l.size
//			
//	  //String repKey = rep.name +"|" + rep.mentionType + "|"+ 
//	  //	rep.number.charAt(0) + "|" + rep.gender.charAt(0) + "|" + rep.animacity.charAt(0);
//	  //String repKey = rep.name;
//			
//	  val hs = HashSet[String]()
//	  for (mentionID <- 0 until l.size) {
//	    val c = l(mentionID)
//				
//		//corefClusterID, positionFrom, positionTo, mentionSpan
//				
//	    val m = SentenceMention(
//	      mentionID = mentionID,
//	      headIndex = c.headPos,
//	      startIndex = c.startPos,
//	      endIndex = c.endPos,
//	      sentNum = c.sentNum,
//	      corefClusterID = c.corefClusterID,
//	      positionFrom = c.positionFrom,
//	      positionTo = c.positionTo,
//	      mentionSpan = c.mentionSpan,
//	      mentionType = c.mentionType,
//	      number = c.number,
//	      gender = c.gender,
//	      animacy = c.animacy,
//	      representativeMentionID = repMentionID,
//	      representativeKey = rep.name
//	    )
//				
//// used to have two tables: all mentions, selected (proper & non-duplicate)
////				w1.write(mentionID + "\t" + c.sentenceID + "\t" + c.headPos + "\t" + c.startPos + "\t" + c.endPos + "\t" +  
////						rep.mentionID + "\t" + c.mentionType + "\t" + c.number + "\t" + c.gender + "\t" + c.animacy + "\n"); //"\t" + repKey + "\n"); //mentionSpan + "\t" + mentionType + "\t" + number + "\t" + gender + "\t" + animacity + "\n");			
////				
////				String ln = c.sentenceID + "\t" + c.headPos + "\t" + repKey + "\n";
////				if (rep.mentionType.equals("PROPER") && !hs.contains(ln)) {
////					w2.write(ln);
////					hs.add(ln);
////				}
//
//	    val ln = m.sentNum + '\t' + m.headIndex + '\t' + m.representativeKey
//	    val OK = rep.mentionType.equals("PROPER") && !hs.contains(ln)
//	    hs += ln
//	    if (OK) {
//		  var lm = sentNum2mentions.getOrElse(m.sentNum, {
//		      var a = ArrayBuffer[SentenceMention]()
//		      sentNum2mentions.put(m.sentNum, a)
//		      a
//		    }
//		  )
//		  lm += m
//	    }
//	  }
//	}	
//	
//    for (sentNum <- 0 until ca.sents.length) {
//	  val lm = sentNum2mentions.getOrElse(sentNum, {
//	    Seq[SentenceMention]()
//	  })
//	
//	  //var ps = 
//	  //  if (lm == null) Array[Mention]()
//	  //  else lm.toArray
//	  lps += lm.toArray
//    }
//    val sma = SentenceMentionAnn(lps.toArray)
//    
//    // we now have SentenceMentionAnn, convert to MentionAnn
//    val ma = convert(sma, stoa)
//    Tuple2(sma, ma)
//  }
//  
//  def convert(sma:SentenceMentionAnn, stoa:SentenceTokenOffsetAnn):MentionAnn = {
//    val ms = ArrayBuffer[Mention]()
//    for (i <- 0 until stoa.sents.size) {
//      val off = stoa.sents(i).f
//      for (m <- sma.sents(i))
//        ms += Mention(
//            m.mentionID,
//            m.headIndex + off,
//            Offsets(m.startIndex + off, m.endIndex + off),
//            m.mentionSpan,
//            m.corefClusterID,
//            m.mentionType.endsWith("PROPER"),
//            (m.representativeMentionID == m.mentionID)
//            //m.representativeKey //m.mentionSpan
//        )
//    }
//    MentionAnn(ms.toArray)
//  }
//
//  case class FeaturizedMention(
//	great:Boolean,
//	proper:Boolean,
//    name:String,
//	mentionID:Int = -1,
//	startPos:Int,
//	endPos:Int,
//	headPos:Int,
//	sentNum:Int,
//	positionFrom:Int,
//	positionTo:Int,
//	corefClusterID:Int,
//	mentionSpan:String,
//	mentionType:String,
//	number:String,
//	gender:String,
//	animacy:String
//  )
//}