package com.readr.spark.malt

import java.io.File
import java.net.URI
import java.util.Properties
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.JavaConversions._
//import com.readr.protobuf.text.TextProto
//import com.readr.protobuf.token.TokenProto
import com.readr.model._
import com.readr.model.annotation._
import com.readr.spark.util.Annotator
import scala.collection.mutable._
import org.maltparser.MaltParserService
import org.maltparser.core.exception.MaltChainedException
import org.maltparser.core.symbol.SymbolTable
import org.maltparser.core.syntaxgraph.DependencyStructure
import org.maltparser.core.syntaxgraph.edge.Edge
import org.maltparser.core.syntaxgraph.node.TokenNode;

// java -jar maltparser-1.7.2.jar -c test -i examples/data/talbanken05_test.conll -o out.conll -m parse

// not sure if useful, but this was used on pardosa07
// java -Xmx1024m -jar malt.jar -w tmp30 -c engmalt -ic utf-8 -i /projects/pardosa/data14/raphaelh/riedel0506/recreate/t3/x00.in -oc utf-8 -of conllx -o /projects/pardosa/data14/raphaelh/riedel0506/recreate/t3/x00.out -m parse &

class MaltParser extends Annotator(
      generates = Array(classOf[SentenceDependencyAnn]),
      requires = Array(classOf[SentenceOffsetAnn], classOf[TokenOffsetAnn], classOf[TokensAnn], classOf[POSAnn])) {
  
  val properties = new Properties()
  val modelFile = "engmalt.linear-1.7.mco"
  var workingDir = "."
  val tmpDir = properties.getProperty("tmpDir", "/tmp")
  val readrTmpDir = tmpDir + "/readr-data_" + System.currentTimeMillis
	

	val tmpDirFile = new File(readrTmpDir)
	//if (tmpDirFile.exists()) tmpDirFile.delete();
		
	if (!tmpDirFile.exists) {
	  tmpDirFile.mkdirs
	  val jarURI = com.readr.spark.util.FileUtils.getJarFromClass(classOf[MaltParser]);
	  System.out.println("now extracting");
	  com.readr.spark.util.FileUtils.extractFromZip(jarURI, "engmalt.linear-1.7.mco", readrTmpDir)
	}

    val service = new MaltParserService

	// Inititalize the parser model 'model0' and sets the working
	// directory to '.' and sets the logging file to 'parser.log'

	workingDir = readrTmpDir;
	service.initializeParserModel("-w " + workingDir + " -c "
			+ modelFile + " -m parse");
	// service.initializeParserModel("-c model0 -m parse -w . -lfi parser.log");
	// parser.initializeParserModel("-u " + modelUrl.toString() +
	// " -m parse");

  override def annotate(ins:Any*):Array[Any] = {
    Array(run(ins(0).asInstanceOf[SentenceOffsetAnn],
        ins(1).asInstanceOf[TokenOffsetAnn],
        ins(2).asInstanceOf[TokensAnn], 
        ins(3).asInstanceOf[POSAnn]))
  }
  
  def run(soa:SentenceOffsetAnn, toa:TokenOffsetAnn, ta:TokensAnn, posa:POSAnn):SentenceDependencyAnn = {
    val sps = new ArrayBuffer[Array[Dependency]]()
    val stoa = Helpers.getSentenceTokenOffsetAnn(soa, toa)
    
    for (sen <- 0 until stoa.sents.length) {

	  // this step is necessary to report progress to hadoop,
	  // so that it doesn't kill this task if there is a document
	  // with very many sentences
	  if (reporter != null) reporter.incrementCounter

	  val tokenBegin = stoa.sents(sen).f
	  val tokenEnd = stoa.sents(sen).t
	  
	  val toks = ta.tokens.slice(tokenBegin, tokenEnd)
	  val poss = posa.pos.slice(tokenBegin, tokenEnd)
	  
	  val dps = new ArrayBuffer[Dependency]()

	  try {
	    // Creates an array of tokens, which contains the Swedish
	    // sentence 'Grundavdraget upph??r allts?? vid en taxerad inkomst
	    // p?? 52500 kr.'
	    // in the CoNLL data format.
	    val parserInput = new Array[String](toks.size)
	    for (i <- 0 until toks.size) {
	  	  val t = toks(i)
		  val pos = poss(i)
		  
		  // TODO:was original, now using value!!!
		  parserInput(i) = (i + 1) + "\t" + t + "\t" + "_" + "\t" + pos; // + "\"
	    }

	    // Run parser
	    val outputGraph = service.parse(parserInput)
	    val symbolTable = outputGraph.getSymbolTables().getSymbolTable("DEPREL")
	    for (i <- 0 until toks.size) {
		  val node = outputGraph.getTokenNode(i + 1)
		  for (edge <- node.getHeadEdges) {
		    val from = edge.getSource().getIndex() - 1
		    val to = edge.getTarget().getIndex() - 1
		    val name = edge.getLabelSymbol(symbolTable)
		    dps += Dependency(name, from, to)
		  }
	    }
	  } catch {
		case e:MaltChainedException => {
			// logger.log(Level.WARNING,
			// "MaltParser exception while parsing sentence: " +
			// e.getMessage(), e);
			// don't pass on exception - go on with next sentence
			//continue
			println("MaltParser exception: " + e.getMessage)
		}
	  }
	  sps += dps.toArray
	}
	SentenceDependencyAnn(sps.toArray)
  }
	
  override def close = {
	service.terminateParserModel
  }
}
