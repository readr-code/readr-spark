package com.readr.spark.stanford34

import java.util.Properties

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable.ArrayBuffer

import com.readr.model.annotation.ParseAnn
import com.readr.model.annotation.SentenceOffsetAnn
import com.readr.model.annotation.TextAnn
import com.readr.model.annotation.TokenOffsetAnn
import com.readr.spark.util.Annotator

import edu.stanford.nlp.ling.CoreAnnotations.SentenceIndexAnnotation
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation
import edu.stanford.nlp.pipeline.{Annotation => StAnnotation}
import edu.stanford.nlp.pipeline.StanfordHelper;
import edu.stanford.nlp.trees.Tree
import edu.stanford.nlp.trees.TreeCoreAnnotations.TreeAnnotation

import com.readr.model.annotation.TokensAnn

object StanfordParser {

  def toStanford(from:ParseAnn, to:StAnnotation):Unit = {
	val l = from.sents
	val sentences = to.get(classOf[SentencesAnnotation])
	for (i <- 0 until l.size) {
	  var tree:Tree = null
	  if (l(i) != null)
	    tree = Tree.valueOf(l(i))
	  sentences.get(i).set(classOf[TreeAnnotation], tree)
	  sentences.get(i).set(classOf[SentenceIndexAnnotation], i.asInstanceOf[Integer])
	}
  }
  
  def fromStanford(from:StAnnotation):ParseAnn = {
	val sentences = from.get(classOf[SentencesAnnotation])
	val l = new ArrayBuffer[String](sentences.length)
	for (sentence <- sentences) {
	  val tree = sentence.get(classOf[TreeAnnotation])
	  val sentNum = sentence.get(classOf[SentenceIndexAnnotation])
	  val ts = if (tree != null) tree.pennString else null
	  l += ts
	}
	ParseAnn(l.toArray)
  }
}

class StanfordParser extends Annotator(
      generates = Array(classOf[ParseAnn]),
      requires = Array(classOf[TextAnn], classOf[SentenceOffsetAnn], classOf[TokenOffsetAnn], classOf[TokensAnn])) {
  
  val properties = new Properties()
  properties.setProperty("parse.maxlen", "100")
  @transient lazy val stanfordAnnotator = StanfordHelper.getAnnotator(properties, "parse") 
  
  override def annotate(ins:Any*):Array[Any] = {
    Array(run(ins(0).asInstanceOf[TextAnn], 
        ins(1).asInstanceOf[SentenceOffsetAnn], 
        ins(2).asInstanceOf[TokenOffsetAnn],
        ins(3).asInstanceOf[TokensAnn]))
  }
  
  def run(t:TextAnn, soa:SentenceOffsetAnn, toa:TokenOffsetAnn, to:TokensAnn):ParseAnn = {
	// create Stanford annotation with relevant contents
	val stanAnn = new StAnnotation(t.text)
	StanfordTokenizer.toStanford(t, toa, to, stanAnn)
	StanfordSentenceSplitter.toStanford(soa, null, stanAnn)
	
	//debug(stanAnn)

	// TODO: stanford parser may take too long for all sentences of a document
	// we must parse sentence by sentence and then report progress using
	//if (reporter != null) reporter.incrementCounter();		
		
	// run stanford annotator
	stanfordAnnotator.annotate(stanAnn)

	// put output back into our annotation
	StanfordParser.fromStanford(stanAnn)
  }
  
  private def debug(stanAnn:edu.stanford.nlp.pipeline.Annotation) = {
	// TODO DEBUG THIS LONG SENTENCE!!!! (ERROR IN TACKBPArticleAnnotator!)
	var l = 0
	val cl = stanAnn.get(classOf[SentencesAnnotation])
	for (c <- cl) {
	  var tl = c.get(classOf[TokensAnnotation]).size
	  //System.out.println(tl);
	  l = Math.max(c.get(classOf[TokensAnnotation]).size, l)
	  if (tl > 100) {
		println("-----LONG SENTENCE:")
		//println(xd.getExtractText().substring(c.get(CharacterOffsetBeginAnnotation.class), c.get(CharacterOffsetEndAnnotation.class)))
	  }
	}
  }
}
