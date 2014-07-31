//package com.readr.spark.stanford34
//
//import java.util.Properties
//import java.util.Set
//import scala.collection.JavaConversions.asScalaBuffer
//import scala.collection.JavaConversions.collectionAsScalaIterable
//import scala.collection.mutable.ArrayBuffer
//import com.readr.model.annotation.CorefAnn
//import com.readr.model.annotation.CorefChain
//import com.readr.model.annotation.SentenceDependencyAnn
//import com.readr.model.annotation.NERTagAnn
//import com.readr.model.annotation.POSAnn
//import com.readr.model.annotation.ParseAnn
//import com.readr.model.annotation.SentenceOffsetAnn
//import com.readr.model.annotation.TextAnn
//import com.readr.model.annotation.TokenOffsetAnn
//import com.readr.spark.util.Annotator
//import edu.stanford.nlp.dcoref.{CorefChain => StCorefChain}
//import edu.stanford.nlp.dcoref.CorefChain.{CorefMention => StCorefMention}
//import edu.stanford.nlp.dcoref.CorefCoreAnnotations.CorefChainAnnotation
//import edu.stanford.nlp.dcoref.Dictionaries
//import edu.stanford.nlp.pipeline.{Annotation => StAnnotation}
//import edu.stanford.nlp.util.IntPair
//import edu.stanford.nlp.util.IntTuple
//import com.readr.model.annotation.TokensAnn
//import com.readr.model.annotation.{CorefMention => CMention}
//
//object StanfordCorefResolver {
//
//  def toStanford(from:CorefAnn, to:StAnnotation):Unit = {
//	val cm = new java.util.HashMap[Integer, StCorefChain]()
//	for (c <- from.chains) {
//			
//	  val mentionMap = new java.util.HashMap[IntPair, Set[StCorefMention]]()
//	  var representative:StCorefMention = null
//			
//	  for (m <- c.mentions) {
//		val com = new StCorefMention(
//		  Dictionaries.MentionType.valueOf(m.mentionTyp.toString),
//		  Dictionaries.Number.valueOf(m.number.toString),
//		  Dictionaries.Gender.valueOf(m.gender.toString),
//		  Dictionaries.Animacy.valueOf(m.animacy.toString),
//		  m.startIndex,
//		  m.endIndex,
//		  m.headIndex,
//		  m.corefClusterID,
//		  m.mentionID,
//		  m.sentNum,
//		  // the arguments here are probably sentNum and headIndex, TODO: verify
//		  new IntTuple(Array[Int](m.positionFrom, m.positionTo)),
//		  m.mentionSpan
//		)
//		val pos = new IntPair(m.sentNum, m.headIndex)
//	    if (!mentionMap.containsKey(pos)) 
//	      mentionMap.put(pos, new java.util.HashSet[StCorefMention]())
//		mentionMap.get(pos).add(com)
//			    
//		if (c.representative == m.mentionID)
//		  representative = com;
//	  }
//	
//	  val cc = new StCorefChain(c.id, mentionMap, representative)
//	  cm.put(c.id, cc)
//	}
//	to.set(classOf[CorefChainAnnotation], cm)
//  }	
//
//  def fromStanford(from:StAnnotation):CorefAnn = {
//    val cca:java.util.Map[Integer,StCorefChain] = from.get(classOf[CorefChainAnnotation])
//    val cl = new ArrayBuffer[CorefChain](cca.size)
//    for (cc <- cca.values) {
//      val l = cc.getMentionsInTextualOrder
//      val lp = new ArrayBuffer[CMention](l.size)
//      for (m <- l) {
//        
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
//      }
//
//      val cpc = CorefChain(cc.getChainID, 
//          cc.getRepresentativeMention().mentionID,
//          lp.toArray)
//      cl += cpc 
//    }
//    
//    CorefAnn(cl.toArray)
//  }
//}
//
//class StanfordCorefResolver extends Annotator(
//  generates = Array(classOf[CorefAnn]),
//  requires = Array(classOf[TextAnn], classOf[TokenOffsetAnn], classOf[TokensAnn], classOf[SentenceOffsetAnn], 
//    classOf[POSAnn], classOf[NERTagAnn], classOf[ParseAnn], classOf[SentenceDependencyAnn])) {
//  
//  val properties = new Properties()
//  
//  // make sure StanfordCoreNLP has parse annotator, which is needed by dcoref
//  StanfordHelper.getAnnotator(properties, "parse")
//  @transient lazy val stanfordAnnotator = StanfordHelper.getAnnotator(properties, "dcoref")
//  
//  override def annotate(ins:Any*):Array[Any] = {
//    Array(run(
//      ins(0).asInstanceOf[TextAnn],
//      ins(1).asInstanceOf[TokenOffsetAnn],
//      ins(2).asInstanceOf[TokensAnn],
//      ins(3).asInstanceOf[SentenceOffsetAnn],
//      ins(4).asInstanceOf[POSAnn],
//      ins(5).asInstanceOf[NERTagAnn],
//      ins(6).asInstanceOf[ParseAnn],
//      ins(7).asInstanceOf[SentenceDependencyAnn]))
//  }
//  
//  def run(t:TextAnn, toa:TokenOffsetAnn, to:TokensAnn, soa:SentenceOffsetAnn, posa:POSAnn, nerta:NERTagAnn, 
//      pa:ParseAnn, sda:SentenceDependencyAnn):CorefAnn = {
//    
//	// create Stanford annotation with relevant contents
//	val stanAnn = new StAnnotation(t.text)
//	StanfordTokenizer.toStanford(t, toa, to, stanAnn)
//	StanfordSentenceSplitter.toStanford(soa, null, stanAnn)
//	StanfordPOSTagger.toStanford(posa, stanAnn)
//	StanfordNERTagger.toStanford(nerta, stanAnn)
//	StanfordParser.toStanford(pa, stanAnn)
//	StanfordDependencyExtractor.toStanford("DepCollapsed", sda, stanAnn)
//
//	// run stanford annotator
//	stanfordAnnotator.annotate(stanAnn)
//
//	StanfordCorefResolver.fromStanford(stanAnn)
//  }
//}
