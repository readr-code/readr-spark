//package com.readr.spark.stanford34
//
//import org.apache.spark.SparkContext
//import org.apache.spark.SparkContext._
//import scala.collection.JavaConversions._
//import com.readr.model._
//import com.readr.model.annotation._
//import edu.stanford.nlp.ling.CoreAnnotations
//import edu.stanford.nlp.ling.CoreAnnotations.CharacterOffsetBeginAnnotation
//import edu.stanford.nlp.ling.CoreAnnotations.CharacterOffsetEndAnnotation
//import edu.stanford.nlp.ling.CoreLabel
//import edu.stanford.nlp.pipeline.{Annotation => StAnnotation}
//import com.readr.spark.util.Annotator
//import java.util._
//import scala.collection.mutable._
//import com.sun.jmx.snmp.defaults.DefaultPaths
//import edu.stanford.nlp.util.PropertiesUtils
//
//object StanfordNERTagger {
//  
//  def toStanford(from:NERTagAnn, to:StAnnotation):Unit = {
//	val li = to.get(classOf[CoreAnnotations.TokensAnnotation])
//	for (i <- 0 until li.size) {
//	  val ner = from.tokens(i)
//	  li.get(i).setNER(ner)
//	}
//  }
//  
//  def fromStanford(from:StAnnotation):NERTagAnn = {
//	val tokens = from.get(classOf[CoreAnnotations.TokensAnnotation])
//	val li = new ArrayBuffer[String](tokens.size)
//	for (cl <- tokens) {
//	  // there may be *NL* tokens outside sentences; the lemmatizer didn't reach
//	  // these, so set these manually to *NL*, so that serialization is OK
//	  var n = cl.ner
//	  if (n == null) n = "O"
//	  li += n
//    }
//	NERTagAnn(li.toArray)
//  }  
//}
//
//class StanfordNERTagger extends Annotator(
//      generates = Array(classOf[NERTagAnn]),
//      requires = Array(classOf[TextAnn], classOf[TokenOffsetAnn], classOf[TokensAnn], classOf[SentenceOffsetAnn], classOf[LemmaAnn], classOf[POSAnn])) {
//  
//  val properties = new Properties()
//  @transient lazy val ner:NERClassifierCombiner = getNer
//  
//  private def getNer:NERClassifierCombiner = {
//  	val models = List[String]()
//	val modelNames = DefaultPaths.DEFAULT_NER_THREECLASS_MODEL + "," + 
//			DefaultPaths.DEFAULT_NER_MUC_MODEL + "," + DefaultPaths.DEFAULT_NER_CONLL_MODEL
//	if (modelNames.length > 0) {
//	  models.addAll(Arrays.asList(modelNames.split(",")));
//	}
//	if (models.isEmpty()) {
//	                // Allow for no real NER model - can just use numeric classifiers or SUTime
//	                // Will have to explicitly unset ner.model.3class, ner.model.7class, ner.model.MISCclass
//	                // So unlikely that people got here by accident
//	                System.err.println("WARNING: no NER models specified");
//	}
//	
//	val applyNumericClassifiers:Boolean =
//	  PropertiesUtils.getBool(properties,
//	                      NERClassifierCombiner.APPLY_NUMERIC_CLASSIFIERS_PROPERTY,
//	                      NERClassifierCombiner.APPLY_NUMERIC_CLASSIFIERS_DEFAULT);
//	val useSUTime:Boolean =
//	  PropertiesUtils.getBool(properties,
//	                      NumberSequenceClassifier.USE_SUTIME_PROPERTY,
//	                      NumberSequenceClassifier.USE_SUTIME_DEFAULT);
//	val nerCombiner = new NERClassifierCombiner(applyNumericClassifiers,
//	                      useSUTime, properties,
//	                      models.toArray(new String[models.size]))
//	nerCombiner
//  }
//	
//  
//  override def annotate(ins:Any*):Array[Any] = {
//    Array(run(ins(0).asInstanceOf[TextAnn], 
//        ins(1).asInstanceOf[TokenOffsetAnn], 
//        ins(2).asInstanceOf[TokensAnn],
//        ins(3).asInstanceOf[SentenceOffsetAnn], 
//        ins(4).asInstanceOf[SentenceTokenOffsetAnn], 
//        ins(5).asInstanceOf[LemmaAnn], 
//        ins(6).asInstanceOf[POSAnn]))
//  }
//  
//  def run(t:TextAnn, toa:TokenOffsetAnn, to:TokensAnn, soa:SentenceOffsetAnn, 
//      stoa:SentenceTokenOffsetAnn, la:LemmaAnn, pa:POSAnn):NERTagAnn = {
//
//    // create Stanford annotation with relevant contents
//	val stanAnn = new StAnnotation(t.text)
//	StanfordDocumentPreprocessor.toStanford(t, toa, to, soa, stoa, stanAnn)
//	//StanfordSentenceSplitter.toStanford(soa, null, stanAnn)
//	StanfordPOSTagger.toStanford(pa, stanAnn)
//	StanfordLemmatizer.toStanford(la, stanAnn)
//		
//	
//    if (stanAnn.containsKey(classOf[CoreAnnotations.SentencesAnnotation])) {
//      // classify tokens for each sentence
//      for (sentence <- stanAnn.get(classOf[CoreAnnotations.SentencesAnnotation])) {
//        val tokens = sentence.get(classOf[CoreAnnotations.TokensAnnotation])
//        val output = this.ner.classifySentenceWithGlobalInformation(tokens, annotation, sentence)
//        
//        for (i <- 0 until tokens.size) {
//          // add the named entity tag to each token
//          val neTag = output.get(i).get(classOf[CoreAnnotations.NamedEntityTagAnnotation])
//          val normNeTag = output.get(i).get(classOf[CoreAnnotations.NormalizedNamedEntityTagAnnotation])
//          tokens.get(i).setNER(neTag)
//          if(normNeTag != null) tokens.get(i).set(classOf[CoreAnnotations.NormalizedNamedEntityTagAnnotation], normNeTag)
//          NumberSequenceClassifier.transferAnnotations(output.get(i), tokens.get(i))
//        }
//      }
//    }
//
//	StanfordNERTagger.fromStanford(stanAnn)
//  }
//}
