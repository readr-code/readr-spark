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

object StanfordNERTagger {
  
  def toStanford(from:NERTagAnn, to:StAnnotation):Unit = {
	val li = to.get(classOf[CoreAnnotations.TokensAnnotation])
	for (i <- 0 until li.size) {
	  val ner = from.tokens(i)
	  li.get(i).setNER(ner)
	}
  }
  
  def fromStanford(from:StAnnotation):NERTagAnn = {
	val tokens = from.get(classOf[CoreAnnotations.TokensAnnotation])
	val li = new ArrayBuffer[String](tokens.size)
	for (cl <- tokens) {
	  // there may be *NL* tokens outside sentences; the lemmatizer didn't reach
	  // these, so set these manually to *NL*, so that serialization is OK
	  var n = cl.ner
	  if (n == null) n = "O"
	  li += n
    }
	NERTagAnn(li.toArray)
  }  
}

class StanfordNERTagger extends Annotator(
      generates = Array(classOf[NERTagAnn]),
      requires = Array(classOf[TextAnn], classOf[TokenOffsetAnn], classOf[TokensAnn], classOf[SentenceOffsetAnn], classOf[LemmaAnn], classOf[POSAnn])) {
  
  val properties = new Properties()
  @transient lazy val stanfordAnnotator = StanfordHelper.getAnnotator(properties, "ner") 
  
  override def annotate(ins:Any*):Array[Any] = {
    Array(run(ins(0).asInstanceOf[TextAnn], 
        ins(1).asInstanceOf[TokenOffsetAnn], 
        ins(2).asInstanceOf[TokensAnn], 
        ins(3).asInstanceOf[SentenceOffsetAnn], 
        ins(4).asInstanceOf[LemmaAnn], 
        ins(5).asInstanceOf[POSAnn]))
  }
  
  def run(t:TextAnn, toa:TokenOffsetAnn, to:TokensAnn, soa:SentenceOffsetAnn, la:LemmaAnn, pa:POSAnn):NERTagAnn = {

    // create Stanford annotation with relevant contents
	val stanAnn = new StAnnotation(t.text)
	StanfordTokenizer.toStanford(t, toa, to, stanAnn)
	StanfordSentenceSplitter.toStanford(soa, null, stanAnn)
	StanfordPOSTagger.toStanford(pa, stanAnn)
	StanfordLemmatizer.toStanford(la, stanAnn)
		
	// run stanford annotator
	stanfordAnnotator.annotate(stanAnn);

	StanfordNERTagger.fromStanford(stanAnn)
  }
}
