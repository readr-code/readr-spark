package com.readr.spark.stanford34

import java.util
import java.util.Properties
import java.util.Set

import edu.stanford.nlp.ling.CoreAnnotations.{TokenBeginAnnotation, SentencesAnnotation}

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.collectionAsScalaIterable
import scala.collection.mutable.ArrayBuffer

import com.readr.model.annotation.MentionAnn
import com.readr.model.annotation.CoreferenceAnn
import com.readr.model.annotation.CoreferenceChain
import com.readr.model.annotation.SentenceDependencyAnn
import com.readr.model.annotation.NERTagAnn
import com.readr.model.annotation.POSAnn
import com.readr.model.annotation.ParseAnn
import com.readr.model.annotation.SentenceOffsetAnn
import com.readr.model.annotation.SentenceTokenOffsetAnn
import com.readr.model.annotation.TextAnn
import com.readr.model.annotation.TokenOffsetAnn
import com.readr.spark.util.Annotator
import edu.stanford.nlp.dcoref.{CorefChain => StCorefChain}
import edu.stanford.nlp.dcoref.CorefChain.{CorefMention => StCorefMention}

import edu.stanford.nlp.dcoref.CorefCoreAnnotations.CorefChainAnnotation
import edu.stanford.nlp.dcoref.Dictionaries
import edu.stanford.nlp.pipeline.{Annotation => StAnnotation}
import edu.stanford.nlp.pipeline.StanfordHelper;
import edu.stanford.nlp.util.{CoreMap, IntPair, IntTuple}

import com.readr.model.Offsets
import com.readr.model.annotation.TokensAnn
import com.readr.model.annotation.Mention
//import com.readr.model.annotation.{CorefMention => CMention}

object StanfordCoreferenceResolver {

  def toStanford(fromT:TextAnn, fromO:TokenOffsetAnn, fromS:SentenceTokenOffsetAnn, 
      fromM:MentionAnn, fromC:CoreferenceAnn, to:StAnnotation):Unit = {
	val cm = new java.util.HashMap[Integer, StCorefChain]()
	val mentions = fromM.mentions
	for (c <- fromC.chains) {
			
	  val mentionMap = new java.util.HashMap[IntPair, Set[StCorefMention]]()
	  var representative:StCorefMention = null
			
	  for (mentionNum <- c.mentionNums) {
	    val m = mentions(mentionNum)
	    
	    // determine sentNum and sentHead
	    var sentNum = 0
	    var sentHead = -1
	    while (sentHead == -1 && sentNum < fromS.sents.size) {
	      if (fromS.sents(sentNum).f <= m.head && m.head < fromS.sents(sentNum).t) {
	        sentHead = m.head - fromS.sents(sentNum).f
	      } else
	        sentNum += 1
	    }
	    val mentionSpan = fromT.text.substring(fromO.tokens(m.tokenOffsets.f).f, fromO.tokens(m.tokenOffsets.t - 1).t)
      sentNum += 1
	    
		val com = new StCorefMention(
		  Dictionaries.MentionType.valueOf(Mention.typeFromByte(m.mentionTyp)),
		  Dictionaries.Number.valueOf(Mention.numberFromByte(m.number)),
		  Dictionaries.Gender.valueOf(Mention.genderFromByte(m.gender)),
		  Dictionaries.Animacy.valueOf(Mention.animacyFromByte(m.animacy)),
		  m.tokenOffsets.f - fromS.sents(sentNum).f +1,
		  m.tokenOffsets.t - fromS.sents(sentNum).f +1, // -1??
		  sentHead,
		  c.chainNum,
		  mentionNum,
		  sentNum,
		  // the arguments here are probably sentNum and headIndex, TODO: verify
		  new IntTuple(Array[Int](sentNum, sentHead)),
		  //new IntTuple(Array[Int](m.positionFrom, m.positionTo)),
		  mentionSpan
		)
		val pos = new IntPair(sentNum, sentHead)
	    if (!mentionMap.containsKey(pos)) 
	      mentionMap.put(pos, new java.util.HashSet[StCorefMention]())
		mentionMap.get(pos).add(com)
			    
		if (c.representativeMentionNum == mentionNum)
		  representative = com
	  }
	
	  val cc = new StCorefChain(c.chainNum, mentionMap, representative)
	  cm.put(c.chainNum, cc)
	}
	to.set(classOf[CorefChainAnnotation], cm)
  }	

  def fromStanford(from:StAnnotation):(MentionAnn,CoreferenceAnn) = {
    val ms = new ArrayBuffer[Mention]()
    val cl = new ArrayBuffer[CoreferenceChain]()
    try {
    val cca:java.util.Map[Integer,StCorefChain] = from.get(classOf[CorefChainAnnotation])

    val sents: util.List[CoreMap] = from.get(classOf[SentencesAnnotation])

    var chainNum = 0
    var mentionNum = 0
    for (cc <- cca.values) {
      val l = cc.getMentionsInTextualOrder
      //val lp = new ArrayBuffer[CMention](l.size)
      
      var representativeMentionNum = -1
      val chainMentions = new ArrayBuffer[Int]()
      for (m <- l) {
        
//        val cpm = CMention(
//		  m.mentionType.name,
//		  m.number.name,
//		  m.gender.name,
//		  m.animacy.name,
//		  m.startIndex,
//		  m.endIndex,
//		  m.headIndex,
//		  m.corefClusterID,
//		  m.mentionID,
//		  m.sentNum,
//		  m.position.get(0),
//		  m.position.get(1),
//		  m.mentionSpan)
//
//		lp += cpm
        val sentTokenBegin: Integer = sents(m.sentNum-1).get(classOf[TokenBeginAnnotation])

		ms += Mention(mentionNum,
		  sentTokenBegin + m.headIndex-1,
		  Offsets(sentTokenBegin + m.startIndex-1, sentTokenBegin + m.endIndex-1),
		  Mention.typeToByte(m.mentionType.name),
		  Mention.numberToByte(m.number.name),
		  Mention.genderToByte(m.gender.name),
		  Mention.animacyToByte(m.animacy.name))

		chainMentions += mentionNum

		if (cc.getRepresentativeMention == m)
		  representativeMentionNum = mentionNum

  		mentionNum += 1		
      }

      cl += CoreferenceChain(chainNum, representativeMentionNum, chainMentions.toArray)
      
      chainNum += 1
//      val cpc = CorefChain(cc.getChainID, 
//          cc.getRepresentativeMention().mentionID,
//          lp.toArray)
//      cl += cpc 
      }
    } catch {
	  case e:Exception =>
      e.printStackTrace()
      println("error in fromStanf")
	}
    (MentionAnn(ms.toArray), CoreferenceAnn(cl.toArray))
  }
}

class StanfordCoreferenceResolver extends Annotator(
  generates = Array(classOf[MentionAnn], classOf[CoreferenceAnn]),
  requires = Array(classOf[TextAnn], classOf[TokenOffsetAnn], classOf[TokensAnn], classOf[SentenceOffsetAnn], 
    classOf[SentenceTokenOffsetAnn],
    classOf[POSAnn], classOf[NERTagAnn], classOf[ParseAnn], classOf[SentenceDependencyAnn])) {
  
  val properties = new Properties()
  
  // make sure StanfordCoreNLP has parse annotator, which is needed by dcoref
  StanfordHelper.getAnnotator(properties, "parse")
  @transient lazy val stanfordAnnotator = StanfordHelper.getAnnotator(properties, "dcoref")
  
  override def annotate(ins:Any*):Array[Any] = {
    val t = run(
      ins(0).asInstanceOf[TextAnn],
      ins(1).asInstanceOf[TokenOffsetAnn],
      ins(2).asInstanceOf[TokensAnn],
      ins(3).asInstanceOf[SentenceOffsetAnn],
      ins(4).asInstanceOf[SentenceTokenOffsetAnn],
      ins(5).asInstanceOf[POSAnn],
      ins(6).asInstanceOf[NERTagAnn],
      ins(7).asInstanceOf[ParseAnn],
      ins(8).asInstanceOf[SentenceDependencyAnn])
    Array(t._1, t._2)
  }
  
  def run(t:TextAnn, toa:TokenOffsetAnn, to:TokensAnn, soa:SentenceOffsetAnn, stoa:SentenceTokenOffsetAnn, posa:POSAnn, nerta:NERTagAnn, 
      pa:ParseAnn, sda:SentenceDependencyAnn):(MentionAnn, CoreferenceAnn) = {
    
	// create Stanford annotation with relevant contents
	val stanAnn = new StAnnotation(t.text)
	try {
	StanfordTokenizer.toStanford(t, toa, to, stanAnn)
	StanfordSentenceSplitter.toStanford(soa, stoa, stanAnn)
	StanfordPOSTagger.toStanford(posa, stanAnn)
	StanfordNERTagger.toStanford(nerta, stanAnn)
	StanfordParser.toStanford(pa, stanAnn)
	} catch {
	  case e:Exception => println("error in earlier toStanford")
	}
	try {
	  StanfordDependencyExtractor.toStanford("DepCollapsed", sda, stanAnn)
	} catch {
	  case e:Exception => {
	      println("error in StanfordDependencyExtractor toStanford")
	      e.printStackTrace()	
	      e.printStackTrace(System.out)
	      StanfordDependencyExtractor.toStanford2("DepCollapsed", sda, stanAnn)
	      
	      System.exit(0)
	  }
	}

	try {
	// run stanford annotator
	stanfordAnnotator.annotate(stanAnn)
	} catch {
	  case e:Exception => println("error in annotator")
	}

	StanfordCoreferenceResolver.fromStanford(stanAnn)
  }
}
