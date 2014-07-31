package com.readr.spark.stanford34

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import scala.collection.JavaConversions._

import com.readr.model._
import com.readr.model.annotation._

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.ling.CoreAnnotations.CharacterOffsetBeginAnnotation
import edu.stanford.nlp.ling.CoreAnnotations.CharacterOffsetEndAnnotation
import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.pipeline.{Annotation => StAnnotation}

import edu.stanford.nlp.pipeline.StanfordHelper;

import com.readr.spark.util.Annotator

import java.util._

import scala.collection.mutable._

object StanfordPOSTagger {
  
  def toStanford(from:POSAnn, to:StAnnotation):Unit = {
	val li = to.get(classOf[CoreAnnotations.TokensAnnotation])
	for (i <- 0 until li.size) {
	  val pos = from.pos(i)
	  li.get(i).set(classOf[CoreAnnotations.PartOfSpeechAnnotation], pos)
	}
  }
  
  def fromStanford(from:StAnnotation):POSAnn = {
	val tokens = from.get(classOf[CoreAnnotations.TokensAnnotation])
	val li = new ArrayBuffer[String](tokens.size)
	for (cl <- tokens)
	  li += cl.getString(classOf[CoreAnnotations.PartOfSpeechAnnotation])
	POSAnn(li.toArray)
  }  
}

class StanfordPOSTagger extends Annotator(
      generates = Array(classOf[POSAnn]),
      requires = Array(classOf[TextAnn], classOf[TokenOffsetAnn], classOf[TokensAnn], classOf[SentenceOffsetAnn])) {
  
  val properties = new Properties()
  @transient lazy val stanfordAnnotator = StanfordHelper.getAnnotator(properties, "pos") 
  
  override def annotate(ins:Any*):Array[Any] = {
    Array(run(ins(0).asInstanceOf[TextAnn],
        ins(1).asInstanceOf[TokenOffsetAnn],
        ins(2).asInstanceOf[TokensAnn],
        ins(3).asInstanceOf[SentenceOffsetAnn]))
  }
  
  def run(t:TextAnn, toa:TokenOffsetAnn, to:TokensAnn, soa:SentenceOffsetAnn):POSAnn = {
	// create Stanford annotation with relevant contents
	val stanAnn:edu.stanford.nlp.pipeline.Annotation = 
	  new edu.stanford.nlp.pipeline.Annotation(t.text)

	StanfordTokenizer.toStanford(t, toa, to, stanAnn)
	StanfordSentenceSplitter.toStanford(soa, null, stanAnn)

	// run stanford annotator
	stanfordAnnotator.annotate(stanAnn)
    
	StanfordPOSTagger.fromStanford(stanAnn)
  }
}
