//package com.readr.spark.stanford34
//
//import org.apache.spark.SparkContext
//import org.apache.spark.SparkContext._
//import scala.collection.JavaConversions._
//import com.readr.model._
//import com.readr.model.annotation._
//import edu.stanford.nlp.ling.CoreAnnotations
//import edu.stanford.nlp.ling.CoreAnnotations.CharacterOffsetBeginAnnotation
//import edu.stanford.nlp.ling.CoreAnnotations.CharacterOffsetEndAnnotation
//import edu.stanford.nlp.ling.CoreLabel
//import edu.stanford.nlp.pipeline.{Annotation => StAnnotation}
//import com.readr.spark.util.Annotator
//import java.util._
//import scala.collection.mutable._
//import edu.stanford.nlp.tagger.maxent.MaxentTagger
//
//object StanfordPOSTagger {
//  
//  def toStanford(from:POSAnn, to:StAnnotation):Unit = {
//	val li = to.get(classOf[CoreAnnotations.TokensAnnotation])
//	for (i <- 0 until li.size) {
//	  val pos = from.pos(i)
//	  li.get(i).set(classOf[CoreAnnotations.PartOfSpeechAnnotation], pos)
//	}
//  }
//  
//  def fromStanford(from:StAnnotation):POSAnn = {
//	val tokens = from.get(classOf[CoreAnnotations.TokensAnnotation])
//	val li = new ArrayBuffer[String](tokens.size)
//	for (cl <- tokens)
//	  li += cl.getString(classOf[CoreAnnotations.PartOfSpeechAnnotation])
//	POSAnn(li.toArray)
//  }  
//}
//
//class StanfordPOSTagger extends Annotator(
//      generates = Array(classOf[POSAnn]),
//      requires = Array(classOf[TextAnn], classOf[TokenOffsetAnn], classOf[TokensAnn], classOf[SentenceOffsetAnn])) {
//
//  val loc = "edu/stanford/nlp/models/pos-tagger/english-left3words/english-left3words-distsim.tagger"
//
//  val properties = new Properties()
//  @transient lazy val pos = new MaxentTagger(loc)
//  
//  override def annotate(ins:Any*):Array[Any] = {
//    Array(run(ins(0).asInstanceOf[TextAnn],
//        ins(1).asInstanceOf[TokenOffsetAnn],
//        ins(2).asInstanceOf[TokensAnn],
//        ins(3).asInstanceOf[SentenceOffsetAnn],
//        ins(4).asInstanceOf[SentenceTokenOffsetAnn]))
//  }
//  
//  def run(t:TextAnn, toa:TokenOffsetAnn, to:TokensAnn, soa:SentenceOffsetAnn, stoa:SentenceTokenOffsetAnn):POSAnn = {
//	// create Stanford annotation with relevant contents
//	val stanAnn:edu.stanford.nlp.pipeline.Annotation = 
//	  new edu.stanford.nlp.pipeline.Annotation(t.text)
//
//    StanfordDocumentPreprocessor.toStanford(t, toa, to, soa, stoa, stanAnn)
//
//	// run stanford annotator
//    
//    for (sentence <- stanAnn.get(classOf[CoreAnnotations.SentencesAnnotation])) {
//      val tokens = sentence.get(classOf[CoreAnnotations.TokensAnnotation]);
//      val tagged = pos.apply(tokens);
//
//      for (i <- 0 until tokens.size)
//        tokens.get(i).set(classOf[CoreAnnotations.PartOfSpeechAnnotation], tagged.get(i).tag)
//    }
//
//	StanfordPOSTagger.fromStanford(stanAnn)
//  }
//}
