package com.readr.spark.stanford34;
//package com.readr.spark.stanfordsr
//
//import java.util.ArrayList
//import java.util.Properties
//
//import scala.collection.JavaConversions.asScalaBuffer
//import scala.collection.mutable.ArrayBuffer
//import scala.util.control.Breaks.break
//import scala.util.control.Breaks.breakable
//
//import com.readr.model.Offsets
//import com.readr.model.annotation.SentenceOffsetAnn
//import com.readr.model.annotation.SentenceTokenOffsetAnn
//import com.readr.model.annotation.TextAnn
//import com.readr.model.annotation.TextFragmentAnn
//import com.readr.model.annotation.TokenOffsetAnn
//import com.readr.spark.util.Annotator
//
//import edu.stanford.nlp.ling.CoreAnnotations.CharacterOffsetBeginAnnotation
//import edu.stanford.nlp.ling.CoreAnnotations.CharacterOffsetEndAnnotation
//import edu.stanford.nlp.ling.CoreAnnotations.SentenceIndexAnnotation
//import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation
//import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation
//import edu.stanford.nlp.ling.CoreAnnotations.TokenBeginAnnotation
//import edu.stanford.nlp.ling.CoreAnnotations.TokenEndAnnotation
//import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation
//import edu.stanford.nlp.ling.CoreLabel
//import edu.stanford.nlp.pipeline.{Annotation => StAnnotation}
//import edu.stanford.nlp.util.CoreMap
//
//import com.readr.model.annotation.TokensAnn
//
//object StanfordSentenceSplitter {
//  
//  // second argument can be null, in which case we compute the token offsets
//  def toStanford(sep:SentenceOffsetAnn, stoa:SentenceTokenOffsetAnn, to:StAnnotation) {
//    val tokens = to.get(classOf[TokensAnnotation])
//	val text = to.get(classOf[TextAnnotation])
//	
//	val sentences = new ArrayList[CoreMap]()
//	var sentNum = 0
//	var nextTok = 0
//	for (i <- 0 until sep.sents.size) {
//	  val s = sep.sents(i)
//			
//	  val sentenceText = text.substring(s.f, s.t)
//
//	  var beginTok = -1
//	  var endTok = -1
//	  if (stoa != null) {
//	    val sto = stoa.sents(i)
//	    beginTok = sto.f
//	    endTok = sto.t
//	  } else {
//	    while (nextTok < tokens.size && tokens.get(nextTok).beginPosition < s.f) nextTok += 1
//	    beginTok = nextTok
//	    endTok = beginTok
//	    while (endTok < tokens.size && tokens.get(endTok).endPosition <= s.t) endTok += 1
//	    nextTok = endTok
//	  }
//	  
//	  val toks = to.get(classOf[TokensAnnotation]).subList(beginTok, endTok)
//			
//	  val sentence = new StAnnotation(sentenceText)
//	  sentence.set(classOf[SentenceIndexAnnotation], sentNum.asInstanceOf[Integer])
//	  sentence.set(classOf[CharacterOffsetBeginAnnotation], s.f.asInstanceOf[Integer])
//	  sentence.set(classOf[CharacterOffsetEndAnnotation], s.t.asInstanceOf[Integer])
//	  sentence.set(classOf[TokensAnnotation], toks)
//	  sentence.set(classOf[TokenBeginAnnotation], beginTok.asInstanceOf[Integer])
//	  sentence.set(classOf[TokenEndAnnotation], endTok.asInstanceOf[Integer])
//	  sentences.add(sentence)
//	  sentNum += 1
//	}
//	to.set(classOf[SentencesAnnotation], sentences)
//  }
//	
//  def fromStanford(from:StAnnotation):(SentenceOffsetAnn, SentenceTokenOffsetAnn) = {
//	val sentences = from.get(classOf[SentencesAnnotation])		
//	val cli = new ArrayBuffer[Offsets](sentences.size)
//	val tli = new ArrayBuffer[Offsets](sentences.size)
//	for (sentence <- sentences) {
//	  cli += Offsets(sentence.get(classOf[CharacterOffsetBeginAnnotation]), 
//	      sentence.get(classOf[CharacterOffsetEndAnnotation]))
//	      
//	  tli += Offsets(sentence.get(classOf[TokenBeginAnnotation]), 
//	      sentence.get(classOf[TokenEndAnnotation]))
//	}
//	(SentenceOffsetAnn(cli.toArray), SentenceTokenOffsetAnn(tli.toArray))
//  }
//}
//
//class StanfordSentenceSplitter extends Annotator(
//      generates = Array(classOf[SentenceOffsetAnn]),
//      requires = Array(classOf[TextAnn], classOf[TextFragmentAnn], classOf[TokenOffsetAnn], classOf[TokensAnn])) {
//  
//  val properties = new Properties()
//  @transient lazy val stanfordAnnotator = StanfordHelper.getAnnotator(properties, "ssplit") 
//  @transient lazy val tokenAnnotator = StanfordHelper.getAnnotator(properties, "tokenize")  //???
//  
//  override def annotate(ins:Any*):Array[Any] = {
//    val t = run(ins(0).asInstanceOf[TextAnn], 
//        ins(1).asInstanceOf[TextFragmentAnn],
//        ins(2).asInstanceOf[TokenOffsetAnn],
//        ins(3).asInstanceOf[TokensAnn])
//    Array(t._1, t._2)
//  }
//  
//  def run(t:TextAnn, fa:TextFragmentAnn, td:TokenOffsetAnn, to:TokensAnn):(SentenceOffsetAnn, SentenceTokenOffsetAnn) = {
//	// create Stanford annotation with relevant contents
//	val stanAnn = new StAnnotation(t.text)
//	StanfordTokenizer.toStanford(t, td, to, stanAnn)
//
//	val docSnts = new ArrayList[CoreMap]()
//	val li = stanAnn.get(classOf[TokensAnnotation])
//	var sentNum = 0
//	
//	// look at every fragment separately
//	for (frag <- fa.frags) {
//	  val raw = t.text.substring(frag.offsets.f, frag.offsets.t)
//			
//	  // get tokens annotations
//	  val sli = new ArrayList[CoreLabel]()
//	  var firstToken = -1
//	  for (i <- 0 until li.size) {
//		val cl = li.get(i);
//		if (cl.get(classOf[CharacterOffsetBeginAnnotation]) >= frag.offsets.f &&
//				cl.get(classOf[CharacterOffsetEndAnnotation]) <= frag.offsets.t) {
//		  if (firstToken == -1) firstToken = i
//					
//		  val ncl = new CoreLabel()
//		  ncl.setValue(cl.value)
//		  ncl.setWord(cl.word)
//		  ncl.setOriginalText(cl.originalText)
//		  ncl.set(classOf[CharacterOffsetBeginAnnotation], new Integer(cl.beginPosition - frag.offsets.f))
//		  ncl.set(classOf[CharacterOffsetEndAnnotation], new Integer(cl.endPosition - frag.offsets.f))
//		  sli.add(ncl)
//		}
//	  }
//	  val fragStanAnn = new StAnnotation(raw)
//	
//	  fragStanAnn.set(classOf[TokensAnnotation], sli)
//			
//	  // now run it
//	  stanfordAnnotator.annotate(fragStanAnn)
//
//	  for (sentence <- fragStanAnn.get(classOf[SentencesAnnotation])) {
//		var sentenceTokens = sentence.get(classOf[TokensAnnotation])
//				
//		// 1. remove newlines at beginning or end of sentence
//		var newStart = 0
//		var newEnd = sentenceTokens.size
//		breakable {
//		  for (i <- 0 until sentenceTokens.size)
//		    if (sentenceTokens.get(i).value().equals("*NL*")) newStart += 1 else break
//		}
//		breakable {
//		  for (i <- sentenceTokens.size-1 to 0 by -1)
//		    if (sentenceTokens.get(i).value().equals("*NL*")) newEnd -= 1 else break
//		}
//
//		// TODO: special case: no tokens left??
//		if (newEnd > newStart) {
//			//if (newStart == 0 && newEnd == sentenceTokens.size()) {
//			//  snts.add(sentence);
//			//  continue;
//			//}
//			//System.out.println(newStart)
//			sentenceTokens = sentenceTokens.subList(newStart, newEnd)
//			sentence.set(classOf[SentenceIndexAnnotation], sentNum.asInstanceOf[Integer])
//			sentence.set(classOf[TokensAnnotation], sentenceTokens)
//			sentence.set(classOf[TokenBeginAnnotation], new Integer(sentence.get(classOf[TokenBeginAnnotation]) + newStart))
//			sentence.set(classOf[TokenEndAnnotation], new Integer(sentence.get(classOf[TokenBeginAnnotation]) + sentenceTokens.size))
//			sentence.set(classOf[CharacterOffsetBeginAnnotation], new Integer(sentenceTokens.get(0).get(classOf[CharacterOffsetBeginAnnotation])))
//			sentence.set(classOf[CharacterOffsetEndAnnotation], new Integer(sentenceTokens.get(sentenceTokens.size-1).get(classOf[CharacterOffsetEndAnnotation])))
//					
//			// 2. correct for document token offsets
//			for (cl <- sentenceTokens) {
//				cl.set(classOf[CharacterOffsetBeginAnnotation], new Integer(cl.get(classOf[CharacterOffsetBeginAnnotation]) + frag.offsets.f))
//				cl.set(classOf[CharacterOffsetEndAnnotation], new Integer(cl.get(classOf[CharacterOffsetEndAnnotation]) + frag.offsets.f))
//			}
//			sentence.set(classOf[CharacterOffsetBeginAnnotation], new Integer(sentence.get(classOf[CharacterOffsetBeginAnnotation]) + frag.offsets.f))
//			sentence.set(classOf[CharacterOffsetEndAnnotation], new Integer(sentence.get(classOf[CharacterOffsetEndAnnotation]) + frag.offsets.f))
//			sentence.set(classOf[TokenBeginAnnotation], new Integer(sentence.get(classOf[TokenBeginAnnotation]) + firstToken))
//			sentence.set(classOf[TokenEndAnnotation], new Integer(sentence.get(classOf[TokenEndAnnotation]) + firstToken))
//					
//			docSnts.add(sentence)
//			sentNum += 1
//		}
//	  }
//	  stanAnn.set(classOf[SentencesAnnotation], docSnts)
//	}
//	
//	StanfordSentenceSplitter.fromStanford(stanAnn)
//  }
//}
