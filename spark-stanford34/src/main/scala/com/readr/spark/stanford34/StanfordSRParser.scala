package com.readr.spark.stanford34

// StanfordSRParser is very fast, but needs A LOT of memory
// ~ 4GB per thread
// with less memory it becomes very slow

import java.util.ArrayList
import java.net.URL
import java.net.URLClassLoader
import java.util.Properties
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import com.readr.model.annotation.ParseAnn
import com.readr.model.annotation.SentenceOffsetAnn
import com.readr.model.annotation.SentenceTokenOffsetAnn
import com.readr.model.annotation.TextAnn
import com.readr.model.annotation.TokenOffsetAnn
import com.readr.model.annotation.TokensAnn
import com.readr.spark.util.Annotator
import edu.stanford.nlp.ling.CoreAnnotations.CharacterOffsetBeginAnnotation
import edu.stanford.nlp.ling.CoreAnnotations.CharacterOffsetEndAnnotation
import edu.stanford.nlp.ling.CoreAnnotations.SentenceIndexAnnotation
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation
import edu.stanford.nlp.ling.CoreAnnotations.TokenBeginAnnotation
import edu.stanford.nlp.ling.CoreAnnotations.TokenEndAnnotation
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation
import edu.stanford.nlp.parser.shiftreduce.ShiftReduceParser
import edu.stanford.nlp.pipeline.{Annotation => StAnnotation}
import edu.stanford.nlp.tagger.maxent.MaxentTagger
import edu.stanford.nlp.trees.Tree
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.trees.TreeCoreAnnotations.TreeAnnotation
import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.ling.HasWord

object StanfordSRParser {

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
  
//  def fromStanford(from:StAnnotation):ParseAnn = {
//	val sentences = from.get(classOf[SentencesAnnotation])
//	val l = new ArrayBuffer[String](sentences.length)
//	for (sentence <- sentences) {
//	  val tree = sentence.get(classOf[TreeAnnotation])
//	  val sentNum = sentence.get(classOf[SentenceIndexAnnotation])
//	  val ts = if (tree != null) tree.pennString else null
//	  l += ts
//	}
//	ParseAnn(l.toArray)
//  }
}

class StanfordSRParser extends Annotator(
      generates = Array(classOf[ParseAnn]),
      requires = Array(classOf[TextAnn], classOf[SentenceOffsetAnn], classOf[SentenceTokenOffsetAnn], classOf[TokenOffsetAnn], classOf[TokensAnn])) {

  val modelPath = "edu/stanford/nlp/models/srparser/englishSR.ser.gz"
  val taggerPath = "edu/stanford/nlp/models/pos-tagger/english-left3words/english-left3words-distsim.tagger"
  //val modelPath = "/Users/raphael/readr-other/stanford/sr/edu/stanford/nlp/models/srparser/englishSR.ser.gz"
  //val taggerPath = "/Users/raphael/readr-other/stanford/stanford-postagger-2014-06-16/models/english-left3words-distsim.tagger"

  val properties = new Properties()
  //properties.setProperty("parse.maxlen", "100")
  //@transient lazy val stanfordAnnotator = StanfordHelper.getAnnotator(properties, "parse") 

  @transient lazy val tagger = new MaxentTagger(taggerPath)
  @transient lazy val model = ShiftReduceParser.loadModel(modelPath)

//    @transient lazy val classLoader = new URLClassLoader(
//      Array[URL](new URL("edu/stanford/nlp/tagger/maxent/MaxentTagger")),
//      ClassLoader.getSystemClassLoader().getParent())
//
//    @transient lazy val isolatedModuleClass = classLoader.loadClass (
//        "edu.stanford.nlp.tagger.maxent.MaxentTagger")
//
//    @transient lazy val isolatedObject = isolatedModuleClass.
//        getConstructor (classOf[String]).
//            newInstance (taggerPath)

  override def annotate(ins:Any*):Array[Any] = {
    Array(run(ins(0).asInstanceOf[TextAnn], 
        ins(1).asInstanceOf[SentenceOffsetAnn], 
        ins(2).asInstanceOf[SentenceTokenOffsetAnn], 
        ins(3).asInstanceOf[TokenOffsetAnn],
        ins(4).asInstanceOf[TokensAnn]))
  }
  
  def run(t:TextAnn, soa:SentenceOffsetAnn, stoa:SentenceTokenOffsetAnn, toa:TokenOffsetAnn, to:TokensAnn):ParseAnn = {
	// create Stanford annotation with relevant contents
	val stanAnn = new StAnnotation(t.text)
	//StanfordTokenizer.toStanford(t, toa, to, stanAnn)
	//StanfordSentenceSplitter.toStanford(soa, null, stanAnn)

    StanfordDocumentPreprocessor.toStanford(t, toa, to, soa, stoa, stanAnn)

	//val sentences = new DocumentPreprocessor(new StringReader(t.text))

	//debug(stanAnn)

	// TODO: stanford parser may take too long for all sentences of a document
	// we must parse sentence by sentence and then report progress using
	//if (reporter != null) reporter.incrementCounter();		

	//val sentences = stanAnn.get(classOf[SentencesAnnotation])
	val sentences = stanAnn.get(classOf[CoreAnnotations.SentencesAnnotation])
	val l = new ArrayBuffer[Tree](sentences.size)
	for (sentence <- sentences) {
	  val s = sentence.get(classOf[CoreAnnotations.TokensAnnotation])
	  val tagged = tagger.tagSentence(s)

//	  val tagged = isolatedModuleClass.getMethod ("tagSentence", classOf[ArrayList[CoreLabel]]).
//            invoke (isolatedObject, s).asInstanceOf[java.util.List[_ <: HasWord]]

      val tree = model.apply(tagged)
      l += tree
      //sentence.set(classOf[TreeAnnotation], tree)
	}
	
	// run stanford annotator
	//stanfordAnnotator.annotate(stanAnn)

	// put output back into our annotation
	//StanfordSRParser.fromStanford(stanAnn)
	val ls = new ArrayBuffer[String](sentences.size)
	for (tree <- l) {
	  val ts = if (tree != null) tree.pennString else null
	  ls += ts
	}
	ParseAnn(ls.toArray)
  }  
  
  
//  private def newMaxentTagger(taggerPath:String):Any = {
//    val classLoader = new URLClassLoader(
//      Array[URL](new URL("classpath:edu/stanford/nlp/tagger/maxent/MaxentTagger")),
//      ClassLoader.getSystemClassLoader().getParent())
//
//    val isolatedModuleClass = classLoader.loadClass (
//        "edu.stanford.nlp.tagger.maxent.MaxentTagger")
//
//    val isolatedObject = isolatedModuleClass.
//        getConstructor (classOf[String]).
//            newInstance (taggerPath)
//    isolatedObject
//  }
  
//  private def runMaxentTagger(isolatedModuleClass:Class[_], isolatedObject:Any):Object = {
//    val result = isolatedModuleClass.getMethod (/* Method name */, /* Method parameter types */).
//            invoke (isolatedObject, /* Method arguments */);
//    
//  }
  
  
  
}
