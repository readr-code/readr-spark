//package com.readr.spark.stanford34
//
//import java.util.Properties
//import scala.collection.JavaConversions.asScalaBuffer
//import scala.collection.mutable.ArrayBuffer
//import com.readr.model.annotation.LemmaAnn
//import com.readr.model.annotation.POSAnn
//import com.readr.model.annotation.SentenceOffsetAnn
//import com.readr.model.annotation.TextAnn
//import com.readr.model.annotation.TokenOffsetAnn
//import com.readr.model.annotation.TokensAnn
//import com.readr.spark.util.Annotator
//import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation
//import edu.stanford.nlp.pipeline.{Annotation => StAnnotation}
//import com.readr.model.annotation.SentenceTokenOffsetAnn
//import edu.stanford.nlp.ling.CoreAnnotations
//import edu.stanford.nlp.process.Morphology
//import edu.stanford.nlp.util.CoreMap
//import edu.stanford.nlp.ling.CoreAnnotation
//
//object StanfordLemmatizer {
//  def toStanford(from:LemmaAnn, to:StAnnotation):Unit = {
//	val li = to.get(classOf[TokensAnnotation])
//	for (i <- 0 until from.lemmas.size) {
//	  val lemma = from.lemmas(i)
//	  li.get(i).setLemma(lemma)
//	}
//  }
//  
//  def fromStanford(from:StAnnotation):LemmaAnn = {
//	val tokens = from.get(classOf[TokensAnnotation])	
//	val li = new ArrayBuffer[String](tokens.size)
//	for (cl <- tokens) {
//	  // there may be *NL* tokens outside sentences; the lemmatizer didn't reach
//	  // these, so set these manually to *NL*, so that serialization is OK
//	  var l = cl.lemma()
//	  if (l == null) l = "*NL*"
//	  li += l
//	}
//	LemmaAnn(li.toArray)
//  }
//}
//
//class StanfordLemmatizer extends Annotator(
//      generates = Array(classOf[LemmaAnn]),
//      requires = Array(classOf[TextAnn], classOf[POSAnn], classOf[SentenceOffsetAnn], 
//          classOf[TokenOffsetAnn], classOf[TokensAnn])) {
//  
//  val properties = new Properties()
//
//  val particles = Array("abroad", "across", "after", "ahead", "along", "aside", "away", "around", "back", "down", "forward", "in", "off", "on", "over", "out", "round", "together", "through", "up")
//  //private static final List<String> particles = Arrays.asList(prep);
//
//  override def annotate(ins:Any*):Array[Any] = {
//    Array(run(ins(0).asInstanceOf[TextAnn], ins(1).asInstanceOf[POSAnn],
//        ins(2).asInstanceOf[SentenceOffsetAnn], ins(3).asInstanceOf[SentenceTokenOffsetAnn],
//        ins(4).asInstanceOf[TokenOffsetAnn], ins(5).asInstanceOf[TokensAnn]))
//  }
//  
//  def run(t:TextAnn, poa:POSAnn, soa:SentenceOffsetAnn, stoa:SentenceTokenOffsetAnn, toa:TokenOffsetAnn, to:TokensAnn):LemmaAnn = {
//
//    // create Stanford annotation with relevant contents
//	val stanAnn = new StAnnotation(t.text)
//	StanfordDocumentPreprocessor.toStanford(t, toa, to, soa, stoa, stanAnn)
//	StanfordPOSTagger.toStanford(poa, stanAnn)
//
//	val morphology = new Morphology
//    if (stanAnn.has(classOf[CoreAnnotations.SentencesAnnotation])) {
//      for (sentence <- stanAnn.get(classOf[CoreAnnotations.SentencesAnnotation])) {
//        val tokens = sentence.get(classOf[CoreAnnotations.TokensAnnotation])
//        //System.err.println("Lemmatizing sentence: " + tokens);
//        for (token <- tokens) {
//          val text = token.get(classOf[CoreAnnotations.TextAnnotation])
//          val posTag = token.get(classOf[CoreAnnotations.PartOfSpeechAnnotation])
//          this.addLemma(morphology, classOf[CoreAnnotations.LemmaAnnotation], token, text, posTag)
//        }
//      }
//    }
//	
//	StanfordLemmatizer.fromStanford(stanAnn)
//  }
//  
//  private def addLemma(morpha:Morphology,
//                        ann:Class[_ <: CoreAnnotation[String]],
//                        map:CoreMap, word:String, tag:String):Unit = {
//    if (tag.length > 0) {
//      val phrVerb = phrasalVerb(morpha, word, tag)
//      if (phrVerb == null) {
//        map.set(ann, morpha.lemma(word, tag))
//      } else {
//        map.set(ann, phrVerb)
//      }
//    } else {
//      map.set(ann, morpha.stem(word))
//    }
//  }
//
//  
//  /** If a token is a phrasal verb with an underscore between a verb and a
//   *  particle, return the phrasal verb lemmatized. If not, return null
//   */
//  private def phrasalVerb(morpha:Morphology, word:String, tag:String):String = {
//
//    // must be a verb and contain an underscore
//    assert(word != null)
//    assert(tag != null)
//    if(!tag.startsWith("VB")  || !word.contains("_")) return null
//
//    // check whether the last part is a particle
//    val verb = word.split("_")
//    if(verb.length != 2) return null
//    val particle = verb(1)
//    if (particles.contains(particle)) {
//      val base = verb(0)
//      val lemma = morpha.lemma(base, tag)
//      return lemma + '_' + particle
//    }
//
//    return null
//  }
//}
