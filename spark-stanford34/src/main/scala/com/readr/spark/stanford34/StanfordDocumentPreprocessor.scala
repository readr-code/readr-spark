package com.readr.spark.stanford34


import scala.collection.JavaConversions._

import com.readr.model._
import com.readr.model.annotation._

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.ling.CoreAnnotations.CharacterOffsetBeginAnnotation
import edu.stanford.nlp.ling.CoreAnnotations.CharacterOffsetEndAnnotation
import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.pipeline.{Annotation => StAnnotation}

import com.readr.spark.util.Annotator

import java.util._

import scala.collection.mutable._
import edu.stanford.nlp.process.DocumentPreprocessor

import java.io.StringReader

import edu.stanford.nlp.ling.HasWord
import edu.stanford.nlp.process.PTBTokenizer
import edu.stanford.nlp.process.CoreLabelTokenFactory
import edu.stanford.nlp.ling.CoreAnnotations.SentenceIndexAnnotation
import edu.stanford.nlp.ling.CoreAnnotations.TokenBeginAnnotation
import edu.stanford.nlp.util.CoreMap
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation
import edu.stanford.nlp.ling.CoreAnnotations.TokenEndAnnotation
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation

object StanfordDocumentPreprocessor {

  // TODO: toStanford for TokensAnn
  
  def toStanford(fromText:TextAnn, fromTokenOff:TokenOffsetAnn, fromToken:TokensAnn, 
      fromSentenceOff:SentenceOffsetAnn, fromSentenceTokenOff:SentenceTokenOffsetAnn, to:StAnnotation):Unit = {
    //TODO
    val text = fromText.text
    val tokenOffs = fromTokenOff.tokens
    val tokens = fromToken.tokens
	val li = new ArrayList[CoreLabel](tokenOffs.size)
	for (i <- 0 until tokenOffs.size) {
	  val to = tokenOffs(i)
	  val v = tokens(i)
	  val orig = text.substring(to.f, to.t)
	  val cl = new CoreLabel		
	  cl.setValue(v)
	  cl.setWord(v)
	  cl.setOriginalText(orig)
	  cl.set(classOf[CoreAnnotations.CharacterOffsetBeginAnnotation], to.f.asInstanceOf[Integer])
	  cl.set(classOf[CoreAnnotations.CharacterOffsetEndAnnotation], to.t.asInstanceOf[Integer])
	  li += cl
	}
	to.set(classOf[CoreAnnotations.TokensAnnotation], li)
	
	//val tokens = to.get(classOf[TokensAnnotation])
	//val text = to.get(classOf[TextAnnotation])
	
	val sentences = new ArrayList[CoreMap]()
	var sentNum = 0
	var nextTok = 0
	for (i <- 0 until fromSentenceOff.sents.size) {
	  val s = fromSentenceOff.sents(i)
			
	  val sentenceText = text.substring(s.f, s.t)

	  var beginTok = -1
	  var endTok = -1
	  if (fromSentenceTokenOff != null) {
	    val sto = fromSentenceTokenOff.sents(i)
	    beginTok = sto.f
	    endTok = sto.t
	  } else {
	    while (nextTok < tokens.size && li(nextTok).beginPosition < s.f) nextTok += 1
	    beginTok = nextTok
	    endTok = beginTok
	    while (endTok < tokens.size && li(endTok).endPosition <= s.t) endTok += 1
	    nextTok = endTok
	  }
	  
	  val toks = to.get(classOf[TokensAnnotation]).subList(beginTok, endTok)
			
	  val sentence = new StAnnotation(sentenceText)
	  sentence.set(classOf[SentenceIndexAnnotation], sentNum.asInstanceOf[Integer])
	  sentence.set(classOf[CharacterOffsetBeginAnnotation], s.f.asInstanceOf[Integer])
	  sentence.set(classOf[CharacterOffsetEndAnnotation], s.t.asInstanceOf[Integer])
	  sentence.set(classOf[TokensAnnotation], toks)
	  sentence.set(classOf[TokenBeginAnnotation], beginTok.asInstanceOf[Integer])
	  sentence.set(classOf[TokenEndAnnotation], endTok.asInstanceOf[Integer])
	  sentences.add(sentence)
	  sentNum += 1
	}
	to.set(classOf[SentencesAnnotation], sentences)
  }
  
//  def fromStanford(from:StAnnotation, to:AnnotationSet, tokenLayerID:Int):Unit = {
//    val li = fromStanford(from)
//    to.annotations += tokenLayerID -> Annotation(tokenLayerID, AnnotationType.TokenOffset, li)
//  }
  
  def fromStanford(from:java.lang.Iterable[List[CoreLabel]]):(TokenOffsetAnn, TokensAnn, SentenceOffsetAnn, SentenceTokenOffsetAnn) = {
    val sentenceOffsets = ArrayBuffer[Offsets]()
    val sentenceTokenOffsets = ArrayBuffer[Offsets]()
    val tokenOffsets = ArrayBuffer[Offsets]()
    val tokens = ArrayBuffer[String]()
    for (sen <- from) {
      if (sen.size > 0)
    	sentenceOffsets += Offsets(sen(0).beginPosition, sen(sen.size-1).endPosition)
      else
        sentenceOffsets += Offsets(0,0)
      sentenceTokenOffsets += Offsets(tokens.size, tokens.size + sen.size)
      for (w <- sen) {
        tokenOffsets += Offsets(w.beginPosition, w.endPosition)
        tokens += w.value
      }
    }
    (TokenOffsetAnn(tokenOffsets.toArray), TokensAnn(tokens.toArray), 
        SentenceOffsetAnn(sentenceOffsets.toArray), SentenceTokenOffsetAnn(sentenceTokenOffsets.toArray))
  }  
}

class StanfordDocumentPreprocessor extends Annotator(
      generates = Array(classOf[TokenOffsetAnn], classOf[TokensAnn], classOf[SentenceOffsetAnn], classOf[SentenceTokenOffsetAnn]),
      requires = Array(classOf[TextAnn])) {
  
  
  @transient lazy val tokenizerFactory = PTBTokenizer.factory(new CoreLabelTokenFactory(true), "");
  
  override def annotate(ins:Any*):Array[Any] = {
    val t = run(ins(0).asInstanceOf[TextAnn])
    Array(t._1, t._2, t._3, t._4)
  }
  
  def run(t:TextAnn):(TokenOffsetAnn, TokensAnn, SentenceOffsetAnn, SentenceTokenOffsetAnn) = {    
	// create Stanford annotation with relevant contents
	//val stanAnn = new StAnnotation(t.text)

	val preprocessor = new DocumentPreprocessor(new StringReader(t.text))
	preprocessor.setTokenizerFactory(tokenizerFactory)
	
	StanfordDocumentPreprocessor.fromStanford(preprocessor.asInstanceOf[java.lang.Iterable[List[CoreLabel]]])
  }
}
