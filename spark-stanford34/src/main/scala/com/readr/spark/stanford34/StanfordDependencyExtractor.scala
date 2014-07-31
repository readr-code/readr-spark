package com.readr.spark.stanford34

import java.util.List
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.asScalaSet
import scala.collection.mutable.ArrayBuffer
import com.readr.model.annotation.Dependency
import com.readr.model.annotation.SentenceDependencyAnn
import com.readr.model.annotation.ParseAnn
import com.readr.model.annotation.SentenceOffsetAnn
import com.readr.model.annotation.TextAnn
import com.readr.model.annotation.TokenOffsetAnn
import com.readr.spark.util.Annotator
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation
import edu.stanford.nlp.ling.CoreAnnotations.TokenBeginAnnotation
import edu.stanford.nlp.ling.CoreAnnotations.TokenEndAnnotation
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation
import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.ling.IndexedWord
import edu.stanford.nlp.pipeline.{Annotation => StAnnotation}
import edu.stanford.nlp.semgraph.SemanticGraph
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations.BasicDependenciesAnnotation
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations.CollapsedDependenciesAnnotation
import edu.stanford.nlp.semgraph.SemanticGraphFactory
import edu.stanford.nlp.trees.GrammaticalRelation
import edu.stanford.nlp.trees.TreeCoreAnnotations.TreeAnnotation
import com.readr.model.annotation.TokensAnn

object StanfordDependencyExtractor {

  val DEFAULT_DEP_TYPE = "DepCCProcessed"//"DepCCProcessed"
  
  val depTypes = Array("DepCollapsed", "DepUncollapsed", "DepCCProcessed")
  
  def toStanford(from:SentenceDependencyAnn, to:StAnnotation):Unit = {
    toStanford(DEFAULT_DEP_TYPE, from, to)
  }
  
  def toStanford(depTyp:String, from:SentenceDependencyAnn, to:StAnnotation):Unit = {
    val toks = to.get(classOf[TokensAnnotation])
	val l = to.get(classOf[SentencesAnnotation])
	for (i <- 0 until l.size) {
	  val fromIndex = l.get(i).get(classOf[TokenBeginAnnotation])
	  val toIndex = l.get(i).get(classOf[TokenEndAnnotation])
	  val sntToks = toks.subList(fromIndex, toIndex)
			
	  val sg = toSemanticGraph(sntToks, from.sents(i))
			
	  depTyp match {
	    case "DepCollapsed" =>
		  l.get(i).set(classOf[CollapsedDependenciesAnnotation], sg)
		case "DepUncollapsed" =>
		  l.get(i).set(classOf[BasicDependenciesAnnotation], sg)
		case "DepCCProcessed" =>
		  l.get(i).set(classOf[CollapsedCCProcessedDependenciesAnnotation], sg)
	  }
	}
  }

  def toStanford2(depTyp:String, from:SentenceDependencyAnn, to:StAnnotation):Unit = {
    println("1")
    val toks = to.get(classOf[TokensAnnotation])
    println("2")
	val l = to.get(classOf[SentencesAnnotation])
    println("3")
	for (i <- 0 until l.size) {
    println("4")
	  val fromIndex = l.get(i).get(classOf[TokenBeginAnnotation])
    println("5")
	  val toIndex = l.get(i).get(classOf[TokenEndAnnotation])
    println("6")
	  val sntToks = toks.subList(fromIndex, toIndex)
    println("7")
			
	  val sg = toSemanticGraph(sntToks, from.sents(i))
    println("8")
			
	  depTyp match {
	    case "DepCollapsed" =>
		  l.get(i).set(classOf[CollapsedDependenciesAnnotation], sg)
		case "DepUncollapsed" =>
		  l.get(i).set(classOf[BasicDependenciesAnnotation], sg)
		case "DepCCProcessed" =>
		  l.get(i).set(classOf[CollapsedCCProcessedDependenciesAnnotation], sg)
	  }
	}
  }

  def toSemanticGraph(tokens:List[CoreLabel], deps:Array[Dependency]):SemanticGraph = {
	val sg = new SemanticGraph()
	for (i <- 0 until tokens.size) {
	  val index = i+1
	  val word = tokens.get(i).value() //getValue();

	  //TODO: not setting root
	  //(are roots those nodes that have 0 incoming edges)
			
      val ifl = new IndexedWord(null, 0, index);
	// condition added by me, after "/" as token caused IndexOutOfBounds, maybe TokensAnnotation in wrong token format?
      val wordAndTag = if (word.length > 1) word.split("/") else Array(word)
      ifl.set(classOf[TextAnnotation], wordAndTag(0))
      if (wordAndTag.length > 1) {
	    ifl.set(classOf[PartOfSpeechAnnotation], wordAndTag(1))
      }
	  sg.addVertex(ifl)
	}
	val vertices = sg.vertexListSorted()
		
	for (d <- deps) {
	  val govId = d.from
	  val reln = d.name
	  val depId = d.to
	  val gov = vertices.get(govId)
	  val dep = vertices.get(depId)
	  val isExtra = false; //?
	  sg.addEdge(gov, dep, GrammaticalRelation.valueOf(reln), 
	      java.lang.Double.NEGATIVE_INFINITY, isExtra);
	}
	return sg;		
  }

  def fromStanford(from:StAnnotation, depTyp:String = DEFAULT_DEP_TYPE):SentenceDependencyAnn = {
	val sentences = from.get(classOf[SentencesAnnotation])
	val psl = new ArrayBuffer[Array[Dependency]](sentences.size)
	for (sentence <- sentences) {
	  val deps = depTyp match {
	    case "DepCollapsed" =>
	  	  sentence.get(classOf[CollapsedDependenciesAnnotation])
	    case "DepUncollapsed" =>
		  sentence.get(classOf[BasicDependenciesAnnotation])
	    case "DepCCProcessed" =>
		  sentence.get(classOf[CollapsedCCProcessedDependenciesAnnotation])
	  }
			
	  // convert semantic graph
	  val edgeSet = deps.getEdgeSet
	  val pl = new ArrayBuffer[Dependency](edgeSet.size)
	  for (e <- edgeSet) {
	    val dep = Dependency(e.getRelation.toString, e.getGovernor.index-1, e.getDependent.index-1)
	    pl += dep
	  }
	  psl += pl.toArray
	}
	SentenceDependencyAnn(psl.toArray)
  }
}

class StanfordDependencyExtractor(depTyp:String = StanfordDependencyExtractor.DEFAULT_DEP_TYPE)
  extends Annotator(
      generates = Array(classOf[SentenceDependencyAnn]),
      requires = Array(classOf[TextAnn], classOf[SentenceOffsetAnn], 
          classOf[TokenOffsetAnn], classOf[TokensAnn], classOf[ParseAnn])) {
  
  //val depTyp:String = StanfordDependencyExtractor.DEFAULT_DEP_TYPE
  //val depTyp:String = "DepCCProcessed" //"DepUncollapsed"

  override def annotate(ins:Any*):Array[Any] = {
    Array(run(ins(0).asInstanceOf[TextAnn], 
        ins(1).asInstanceOf[SentenceOffsetAnn], 
        ins(2).asInstanceOf[TokenOffsetAnn], 
        ins(3).asInstanceOf[TokensAnn], 
        ins(4).asInstanceOf[ParseAnn]))
  }
  
  def run(t:TextAnn, soa:SentenceOffsetAnn, toa:TokenOffsetAnn, to:TokensAnn, pa:ParseAnn):SentenceDependencyAnn = {

    // create Stanford annotation with relevant contents
	val stanAnn = new StAnnotation(t.text)
	StanfordTokenizer.toStanford(t, toa, to, stanAnn)
	StanfordSentenceSplitter.toStanford(soa, null, stanAnn)
	StanfordParser.toStanford(pa, stanAnn)
	
	// run stanford annotator
	for (sentence <- stanAnn.get(classOf[SentencesAnnotation])) {
	  val tree = sentence.get(classOf[TreeAnnotation])

	  depTyp match {
		case "DepCollapsed" => {
		  val deps = SemanticGraphFactory.generateCollapsedDependencies(tree)
		  sentence.set(classOf[CollapsedDependenciesAnnotation], deps)
		}
		case "DepUncollapsed" => {
		  val deps = SemanticGraphFactory.generateUncollapsedDependencies(tree)
		  sentence.set(classOf[BasicDependenciesAnnotation], deps)
		}
		case "DepCCProcessed" => {
		  val deps = SemanticGraphFactory.generateCCProcessedDependencies(tree)
		  sentence.set(classOf[CollapsedCCProcessedDependenciesAnnotation], deps)
		}
	  }
	}
	
	StanfordDependencyExtractor.fromStanford(stanAnn, depTyp)
  }
  
  override def toString = "StanfordDependencyExtractor(depTyp=" + depTyp + ")"
}
