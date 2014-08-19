package com.readr.spark.stanford34

import java.util.Properties

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable.ArrayBuffer

import com.readr.model.annotation.LemmaAnn
import com.readr.model.annotation.POSAnn
import com.readr.model.annotation.SentenceOffsetAnn
import com.readr.model.annotation.TextAnn
import com.readr.model.annotation.TokenOffsetAnn
import com.readr.model.annotation.TokensAnn
import com.readr.spark.util.Annotator

import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation
import edu.stanford.nlp.pipeline.StanfordHelper;
import edu.stanford.nlp.pipeline.{Annotation => StAnnotation}

object StanfordLemmatizer {
  def toStanford(from:LemmaAnn, to:StAnnotation):Unit = {
	val li = to.get(classOf[TokensAnnotation])
	for (i <- 0 until from.lemmas.size) {
	  val lemma = from.lemmas(i)
	  li.get(i).setLemma(lemma)
	}
  }
  
  def fromStanford(from:StAnnotation):LemmaAnn = {
	val tokens = from.get(classOf[TokensAnnotation])	
	val li = new ArrayBuffer[String](tokens.size)
	for (cl <- tokens) {
	  // there may be *NL* tokens outside sentences; the lemmatizer didn't reach
	  // these, so set these manually to *NL*, so that serialization is OK
	  var l = cl.lemma()
	  if (l == null) l = "*NL*"
	  li += l
	}
	LemmaAnn(li.toArray)
  }
}

class StanfordLemmatizer extends Annotator(
      generates = Array(classOf[LemmaAnn]),
      requires = Array(classOf[TextAnn], classOf[POSAnn], classOf[SentenceOffsetAnn], 
          classOf[TokenOffsetAnn], classOf[TokensAnn])) {
  
  val properties = new Properties()
  @transient lazy val stanfordAnnotator = StanfordHelper.getAnnotator(properties, "lemma") 
  
  override def annotate(ins:Any*):Array[Any] = {
    Array(run(ins(0).asInstanceOf[TextAnn], ins(1).asInstanceOf[POSAnn],
        ins(2).asInstanceOf[SentenceOffsetAnn], ins(3).asInstanceOf[TokenOffsetAnn],
        ins(4).asInstanceOf[TokensAnn]))
  }
  
  def run(t:TextAnn, poa:POSAnn, soa:SentenceOffsetAnn, toa:TokenOffsetAnn, to:TokensAnn):LemmaAnn = {

    // create Stanford annotation with relevant contents
	val stanAnn = new StAnnotation(t.text)
	StanfordTokenizer.toStanford(t, toa, to, stanAnn)
	StanfordSentenceSplitter.toStanford(soa, null, stanAnn)
	StanfordPOSTagger.toStanford(poa, stanAnn)

	// run stanford annotator
	stanfordAnnotator.annotate(stanAnn)
    
	StanfordLemmatizer.fromStanford(stanAnn)
  }
}
