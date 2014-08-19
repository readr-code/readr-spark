//package com.readr.spark.stanford34
//
//
//import scala.collection.JavaConversions._
//
//import com.readr.model._
//import com.readr.model.annotation._
//
//import edu.stanford.nlp.ling.CoreAnnotations
//import edu.stanford.nlp.ling.CoreAnnotations.CharacterOffsetBeginAnnotation
//import edu.stanford.nlp.ling.CoreAnnotations.CharacterOffsetEndAnnotation
//import edu.stanford.nlp.ling.CoreLabel
//import edu.stanford.nlp.pipeline.{Annotation => StAnnotation}
//
//import com.readr.spark.stanford34.StanfordTokenizer;
//import com.readr.spark.util.Annotator
//
//import java.util._
//
//import scala.collection.mutable._
//import edu.stanford.nlp.process.PTBTokenizer
//import edu.stanford.nlp.process.CoreLabelTokenFactory
//
//import java.io.StringReader
//
//object StanfordTokenizer {
//
//  // TODO: toStanford for TokensAnn
//  
//  def toStanford(fromText:TextAnn, fromTokenOff:TokenOffsetAnn, fromToken:TokensAnn, to:StAnnotation):List[CoreLabel] = {
//    val text = fromText.text
//    val tokenOffs = fromTokenOff.tokens
//    val tokens = fromToken.tokens
//	val li = new ArrayList[CoreLabel](tokenOffs.size)
//	for (i <- 0 until tokenOffs.size) {
//	  val to = tokenOffs(i)
//	  val v = tokens(i)
//	  val orig = text.substring(to.f, to.t)
//	  val cl = new CoreLabel		
//	  cl.setValue(v)
//	  cl.setWord(v)
//	  cl.setOriginalText(orig)
//	  cl.set(classOf[CoreAnnotations.CharacterOffsetBeginAnnotation], to.f.asInstanceOf[Integer])
//	  cl.set(classOf[CoreAnnotations.CharacterOffsetEndAnnotation], to.t.asInstanceOf[Integer])
//	  li += cl
//	}
//	to.set(classOf[CoreAnnotations.TokensAnnotation], li)
//  }
//  
////  def fromStanford(from:StAnnotation, to:AnnotationSet, tokenLayerID:Int):Unit = {
////    val li = fromStanford(from)
////    to.annotations += tokenLayerID -> Annotation(tokenLayerID, AnnotationType.TokenOffset, li)
////  }
//  
//  def fromStanford(from:edu.stanford.nlp.process.Tokenizer[edu.stanford.nlp.ling.CoreLabel]):(TokenOffsetAnn, TokensAnn) = {
//    val tokens = from.tokenize
//	//val tokens = from..get(classOf[CoreAnnotations.TokensAnnotation])
//	val li = new ArrayBuffer[Offsets](tokens.size)
//	val tli = new ArrayBuffer[String](tokens.size)
//	for (cl <- tokens) {
//	  li += Offsets(cl.beginPosition, cl.endPosition)
//	  tli += cl.value
//	}
//	(TokenOffsetAnn(li.toArray), TokensAnn(tli.toArray))
//  }  
//}
//
//class StanfordTokenizer extends Annotator(
//      generates = Array(classOf[TokenOffsetAnn], classOf[TokensAnn]),
//      requires = Array(classOf[TextAnn])) {
//  
//  val properties = new Properties()
//  val eolIsSignificant = false
//  @transient lazy val tokenizerFactory = PTBTokenizer.factory(new CoreLabelTokenFactory(true), "");
//  
//  //@transient lazy val stanfordAnnotator = StanfordHelper.getAnnotator(properties, "tokenize") 
//  
//  override def annotate(ins:Any*):Array[Any] = {
//    val t = run(ins(0).asInstanceOf[TextAnn])
//    Array(t._1, t._2)
//  }
//  
//  def run(t:TextAnn):(TokenOffsetAnn, TokensAnn) = {    
//	// create Stanford annotation with relevant contents
//	val stanAnn = new StAnnotation(t.text)
//
//	
//	val tokenizer = tokenizerFactory.getTokenizer(new StringReader(t.text))
//
////	for (cl <- tokenizer) {
////	  cl.
////	}
//	
//	// run stanford annotator
//	//stanfordAnnotator.annotate(stanAnn);
//
//	StanfordTokenizer.fromStanford(tokenizer)
//  }
//}
