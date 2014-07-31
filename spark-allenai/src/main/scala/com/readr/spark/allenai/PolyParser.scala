package com.readr.spark.allenai

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.mutable.HashSet
import scala.collection.JavaConversions._
import com.readr.model._
import com.readr.model.annotation._
import com.readr.spark.util.Annotator
import scala.collection.mutable.ArrayBuffer
import java.util.Properties
import java.util.Vector
//import org.allenai.nlpstack.graph.Graph
//import org.allenai.nlpstack.parse.graph.{ DependencyNode, DependencyGraph }
//import org.allenai.nlpstack.postag.{ PostaggedToken, defaultPostagger }
import org.allenai.nlpstack.tokenize.defaultTokenizer
import org.allenai.parsers.polyparser
import org.allenai.parsers.polyparser.{ NexusToken, WordClusters, Transition }
import org.allenai.common.Resource.using
import java.io.StringWriter
import java.io.PrintWriter
import org.allenai.nlpstack.core.parse.graph.DependencyNode
import org.allenai.nlpstack.core.PostaggedToken
import org.allenai.nlpstack.core.graph.Graph
import org.allenai.nlpstack.core.parse.graph.DependencyGraph


object PolyParser {
  
}

class PolyParser extends Annotator(
      generates = Array(classOf[SentenceDependencyAnn]),
      requires = Array(classOf[TokensAnn], classOf[SentenceTokenOffsetAnn], 
          classOf[TokenOffsetAnn], classOf[POSAnn])) {
  
  @transient lazy val parser = new org.allenai.nlpstack.parse.PolytreeParser
  //@transient lazy val parser = new PolytreeParser

  override def annotate(ins:Any*):Array[Any] = {
    Array(run(ins(0).asInstanceOf[TokensAnn], 
        ins(1).asInstanceOf[SentenceTokenOffsetAnn],
        ins(2).asInstanceOf[TokenOffsetAnn],
		ins(3).asInstanceOf[POSAnn]))
  }

  def run(ta:TokensAnn, stoa:SentenceTokenOffsetAnn, toa:TokenOffsetAnn, pa:POSAnn):SentenceDependencyAnn = {
    
    val postagged = FactoriePOSTagger.toAllenai(pa, ta, toa)

    val deps = new ArrayBuffer[Array[Dependency]]()
    
    for (sen <- stoa.sents) {      
      try {
        var depGraph = parser.dependencyGraphPostagged(postagged.slice(sen.f, sen.t))      
        depGraph = depGraph.collapse
        val senDeps = depGraph.dependencies.map(x => Dependency(x.label, x.source.id, x.dest.id)).toArray        

        //sanity check on deps
        var ok = true
        for (d <- senDeps)
          if (d.from < 0 || d.to < 0)
            ok = false
        
        if (ok)
          deps += senDeps 
        else 
          deps += Array[Dependency]()
        
      } catch {
        case e:Exception =>
          val sw = new StringWriter
          e.printStackTrace(new PrintWriter(sw))
          val m = sw.toString
          if (m.startsWith("java.lang.IllegalArgumentException: There must be a single root")) {
            println("Sentence with multiple roots: ")
            for (i <- sen.f until sen.t) {
              val t = postagged(i)
              print(t.string + " ")
            }
            println
          } else {
            e.printStackTrace            
          }
          deps += Array()
      }
    }    
    SentenceDependencyAnn(deps.toArray)
  }
}

//    InputStream is = getClass().getClassLoader().getResourceAsStream(path);
//    if (is == null)
//      return null;
//    try {
//      if (path.endsWith(".gz"))
//        is = new GZIPInputStream(new BufferedInputStream(is));
//      else
//        is = new BufferedInputStream(is);
//    } catch (IOException e) {
//      System.err.println("CLASSPATH resource " + path + " is not a GZIP stream!");
//    }
//    return is;



//class PolytreeParser {
//  private val parser =
//    new polyparser.GreedyTransitionParser(
//      using( /*Thread.currentThread.getContextClassLoader*/
//        classOf[org.allenai.nlpstack.parse.PolytreeParser].getClassLoader.getResourceAsStream(
//          "org/allenai/polyparser-models/wsj.train.30.dstan3_4.dt.poly.json.gz")) {
//          polyparser.ClassifierBasedCostFunction.loadFromStream(_)
//        })
//  
//  /*override*/ def dependencyGraphPostagged(tokens: Seq[PostaggedToken]) = {
//    val polyTokens = for (token <- tokens) yield {
//      polyparser.Token(
//        Symbol(token.string),
//        Some(Symbol(WordClusters.ptbToUniversalPosTag.getOrElse(token.postag, token.postag))),
//        Some(token.postagSymbol))
//    }
//    val polyTokensVector = NexusToken +: polyTokens.toVector
//    // Polyparser needs a nexus token in its initial state.
//    val initialState =
//      polyparser.TransitionParserState.initialState(polyTokensVector)
//    val transitionsOption = parser.parse(initialState)
//    val parseOption = transitionsOption.map(
//      polyparser.PolytreeParse.fromTransitions(polyTokensVector, _))
//
//    val nodes = for (
//      parse <- parseOption.toList;
//      (token, index) <- parse.tokens.drop(1).zipWithIndex // dropping the nexus token
//    ) yield {
//      DependencyNode(index, token.word.name)
//    }
//
//    val edges = for (
//      parse <- parseOption.toList;
//      ((arclabels, childIndices), parentIndex) <- (parse.arclabels zip parse.children).zipWithIndex;
//      if parentIndex > 0;
//      (childIndex, Symbol(label)) <- arclabels.filter(t => childIndices.contains(t._1))
//    ) yield {
//      new Graph.Edge(nodes(parentIndex - 1), nodes(childIndex - 1), label.toLowerCase)
//    }
//
//    val nodesWithIncomingEdges = edges.map(_.dest).toSet
//    val nodesWithoutIncomingEdges = nodes.toSet -- nodesWithIncomingEdges
//    require(nodesWithoutIncomingEdges.size <= 1, s"Parser output for sentence '${tokens.map(_.string).mkString(" ")}' has multiple roots.")
//
//    DependencyGraph(nodes.toSet, edges.toSet)
//  }
//  

//  /*override */ def dependencyGraphPostagged(tokens: Seq[PostaggedToken]) = {
//    val polyTokens = for (token <- tokens) yield {
//      polyparser.Token(
//        Symbol(token.string),
//        Some(Symbol(WordClusters.ptbToUniversalPosTag.getOrElse(token.postag, token.postag))),
//        Some(token.postagSymbol))
//    }
//    val polyTokensVector = NexusToken +: polyTokens.toVector
//    // Polyparser needs a nexus token in its initial state.
//    val initialState =
//      polyparser.TransitionParserState.initialState(polyTokensVector)
//    val transitionsOption = parser.parse(initialState)
//    val parseOption = transitionsOption.map(
//      polyparser.PolytreeParse.fromTransitions(polyTokensVector, _))
//
//    val nodes = for (
//      parse <- parseOption.toList;
//      (token, index) <- parse.tokens.drop(1).zipWithIndex // dropping the nexus token
//    ) yield {
//      DependencyNode(index, token.word.name)
//    }
//
//    val edges = for (
//      parse <- parseOption.toList;
//      ((arclabels, childIndices), parentIndex) <- (parse.arclabels zip parse.children).zipWithIndex;
//      if parentIndex > 0;
//      (childIndex, Symbol(label)) <- arclabels.filter(t => childIndices.contains(t._1))
//    ) yield {
//      new Graph.Edge(nodes(parentIndex - 1), nodes(childIndex - 1), label)
//    }
//
//    val nodesWithIncomingEdges = edges.map(_.dest).toSet
//    val nodesWithoutIncomingEdges = nodes.toSet -- nodesWithIncomingEdges
//    val firstRoot = nodesWithoutIncomingEdges.toSeq(0)
//
//    DependencyGraph(Some(firstRoot), nodes.toSet, edges.toSet)
//  }
