//package com.readr.spark.stanford34;
package edu.stanford.nlp.pipeline;

import static edu.stanford.nlp.pipeline.Annotator.STANFORD_CLEAN_XML;
import static edu.stanford.nlp.pipeline.Annotator.STANFORD_DETERMINISTIC_COREF;
import static edu.stanford.nlp.pipeline.Annotator.STANFORD_GENDER;
import static edu.stanford.nlp.pipeline.Annotator.STANFORD_LEMMA;
import static edu.stanford.nlp.pipeline.Annotator.STANFORD_NER;
import static edu.stanford.nlp.pipeline.Annotator.STANFORD_PARSE;
import static edu.stanford.nlp.pipeline.Annotator.STANFORD_POS;
import static edu.stanford.nlp.pipeline.Annotator.STANFORD_REGEXNER;
import static edu.stanford.nlp.pipeline.Annotator.STANFORD_RELATION;
import static edu.stanford.nlp.pipeline.Annotator.STANFORD_SENTIMENT;
import static edu.stanford.nlp.pipeline.Annotator.STANFORD_SSPLIT;
import static edu.stanford.nlp.pipeline.Annotator.STANFORD_TOKENIZE;
import static edu.stanford.nlp.pipeline.Annotator.STANFORD_TRUECASE;
//import static edu.stanford.nlp.pipeline.AnnotatorFactory.baseSignature;
import static edu.stanford.nlp.pipeline.StanfordCoreNLP.CUSTOM_ANNOTATOR_PREFIX;
import static edu.stanford.nlp.pipeline.StanfordCoreNLP.DEFAULT_NEWLINE_IS_SENTENCE_BREAK;
import static edu.stanford.nlp.pipeline.StanfordCoreNLP.NEWLINE_IS_SENTENCE_BREAK_PROPERTY;
import static edu.stanford.nlp.pipeline.StanfordCoreNLP.NEWLINE_SPLITTER_PROPERTY;

import java.io.FileNotFoundException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import edu.stanford.nlp.ie.NERClassifierCombiner;
import edu.stanford.nlp.ie.regexp.NumberSequenceClassifier;
import edu.stanford.nlp.io.RuntimeIOException;
import edu.stanford.nlp.pipeline.Annotator;
import edu.stanford.nlp.pipeline.AnnotatorFactory;
import edu.stanford.nlp.pipeline.AnnotatorPool;
import edu.stanford.nlp.pipeline.CharniakParserAnnotator;
import edu.stanford.nlp.pipeline.CleanXmlAnnotator;
import edu.stanford.nlp.pipeline.DefaultPaths;
import edu.stanford.nlp.pipeline.DeterministicCorefAnnotator;
import edu.stanford.nlp.pipeline.GenderAnnotator;
import edu.stanford.nlp.pipeline.MorphaAnnotator;
import edu.stanford.nlp.pipeline.NERCombinerAnnotator;
import edu.stanford.nlp.pipeline.POSTaggerAnnotator;
import edu.stanford.nlp.pipeline.PTBTokenizerAnnotator;
import edu.stanford.nlp.pipeline.ParserAnnotator;
import edu.stanford.nlp.pipeline.RelationExtractorAnnotator;
import edu.stanford.nlp.pipeline.SentimentAnnotator;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.pipeline.TokensRegexNERAnnotator;
import edu.stanford.nlp.pipeline.TrueCaseAnnotator;
import edu.stanford.nlp.pipeline.WhitespaceTokenizerAnnotator;
import edu.stanford.nlp.pipeline.WordsToSentencesAnnotator;
import edu.stanford.nlp.process.PTBTokenizer;
import edu.stanford.nlp.util.Generics;
import edu.stanford.nlp.util.PropertiesUtils;
import edu.stanford.nlp.util.ReflectionLoading;
//import static edu.stanford.nlp.pipeline.StanfordCoreNLP.STANFORD_NFL;

public class StanfordHelper {

	// we want to avoid creating the entire annotator pool each
	// time we run a stanford annotator, so we duplicate code
	// from class StanfordCoreNLP to create annotators individually 
	
	private static AnnotatorPool pool = null;
	
	public static Annotator getAnnotator(Properties properties, String annotatorName) 
		throws IllegalArgumentException {
		if (!properties.containsKey("annotators"))
			properties.setProperty("annotators", "");

		if (annotatorName.equals("dcoref"))
			properties.setProperty("annotators", "parse");
		
		// this would be the usual way to get an annotator by creating
		// the entire pool
		//StanfordCoreNLP corenlp = new StanfordCoreNLP(properties, false);
		//return StanfordCoreNLP.getExistingAnnotator("ner");

		// we would like to create annotators lazily, i.e. only if we need them
		// we use reflection to access StanfordCoreNLP.pool
		if (pool == null) {
			Field f = null;
			try {
				f = StanfordCoreNLP.class.getDeclaredField("pool");
			} catch (NoSuchFieldException e) {
				e.printStackTrace();
			} catch (SecurityException e) {
				e.printStackTrace();
			}
			f.setAccessible(true);
			try {
				pool = (AnnotatorPool)f.get(null);
				if (pool == null)
					f.set(null, pool = new AnnotatorPool());
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		}
		
		// this is what we do here:
		Annotator annotator;
		try {			
			annotator = pool.get(annotatorName);
		} catch (IllegalArgumentException e) {
			// annotator factory might not have been added to pool yet
			addToPool(properties, annotatorName);
			// note, that this might throw another illegal argument exception
			annotator = pool.get(annotatorName);
		}
		return annotator;
	}
	
	private static void addToPool(final Properties inputProps, String annotatorName) {
		
		
		//
	    // tokenizer: breaks text into a sequence of tokens
	    // this is required for all following annotators!
	    //
	    if (STANFORD_TOKENIZE.equals(annotatorName)) {
	        pool.register(STANFORD_TOKENIZE, new AnnotatorFactory(inputProps) {
	            private static final long serialVersionUID = 1L;
	            @Override
	            public Annotator create() {
	              if (Boolean.valueOf(properties.getProperty("tokenize.whitespace",
	                                "false"))) {
	                return new WhitespaceTokenizerAnnotator(properties);
	              } else {
	                String options = properties.getProperty("tokenize.options", PTBTokenizerAnnotator.DEFAULT_OPTIONS);
	                boolean keepNewline = Boolean.valueOf(properties.getProperty(NEWLINE_SPLITTER_PROPERTY, "false"));
	                // If they
	                if (properties.getProperty(NEWLINE_IS_SENTENCE_BREAK_PROPERTY) != null) {
	                  keepNewline = true;
	                }
	                // If the user specifies "tokenizeNLs=false" in tokenize.options, then this default will
	                // be overridden.
	                if (keepNewline) {
	                  options = "tokenizeNLs," + options;
	                }
	                return new PTBTokenizerAnnotator(false, options);
	              }
	            }

	            @Override
	            public String signature() {
	              // keep track of all relevant properties for this annotator here!
	              StringBuilder os = new StringBuilder();
	              os.append("tokenize.whitespace:" +
	                      properties.getProperty("tokenize.whitespace", "false"));
	              if (properties.getProperty("tokenize.options") != null) {
	                os.append(":tokenize.options:" + properties.getProperty("tokenize.options"));
	              }
	              if (Boolean.valueOf(properties.getProperty("tokenize.whitespace",
	                      "false"))) {
	                os.append(WhitespaceTokenizerAnnotator.EOL_PROPERTY + ":" +
	                        properties.getProperty(WhitespaceTokenizerAnnotator.EOL_PROPERTY,
	                                "false"));
	                os.append(StanfordCoreNLP.NEWLINE_SPLITTER_PROPERTY + ":" +
	                        properties.getProperty(StanfordCoreNLP.NEWLINE_SPLITTER_PROPERTY,
	                                "false"));
	                return os.toString();
	              } else {
	                os.append(NEWLINE_SPLITTER_PROPERTY + ":" +
	                        Boolean.valueOf(properties.getProperty(NEWLINE_SPLITTER_PROPERTY,
	                                "false")));
	                os.append(NEWLINE_IS_SENTENCE_BREAK_PROPERTY + ":" +
	                          properties.getProperty(NEWLINE_IS_SENTENCE_BREAK_PROPERTY, DEFAULT_NEWLINE_IS_SENTENCE_BREAK));
	              }
	              return os.toString();
	            }
	          });
	    }	    

	    
	    if (STANFORD_CLEAN_XML.equals(annotatorName)) {
	        pool.register(STANFORD_CLEAN_XML, new AnnotatorFactory(inputProps) {
	            private static final long serialVersionUID = 1L;
	            @Override
	            public Annotator create() {
	              String xmlTags =
	                properties.getProperty("clean.xmltags",
	                                  CleanXmlAnnotator.DEFAULT_XML_TAGS);
	              String sentenceEndingTags =
	                properties.getProperty("clean.sentenceendingtags",
	                                  CleanXmlAnnotator.DEFAULT_SENTENCE_ENDERS);
	              String singleSentenceTags =
	                      properties.getProperty("clean.singlesentencetags",
	                              CleanXmlAnnotator.DEFAULT_SINGLE_SENTENCE_TAGS);
	              String allowFlawedString = properties.getProperty("clean.allowflawedxml");
	              boolean allowFlawed = CleanXmlAnnotator.DEFAULT_ALLOW_FLAWS;
	              if (allowFlawedString != null)
	                allowFlawed = Boolean.valueOf(allowFlawedString);
	              String dateTags =
	                properties.getProperty("clean.datetags",
	                                  CleanXmlAnnotator.DEFAULT_DATE_TAGS);
	              String docIdTags =
	                      properties.getProperty("clean.docIdtags",
	                              CleanXmlAnnotator.DEFAULT_DOCID_TAGS);
	              String docTypeTags =
	                      properties.getProperty("clean.docTypetags",
	                              CleanXmlAnnotator.DEFAULT_DOCTYPE_TAGS);
	              String utteranceTurnTags =
	                      properties.getProperty("clean.turntags",
	                              CleanXmlAnnotator.DEFAULT_UTTERANCE_TURN_TAGS);
	              String speakerTags =
	                      properties.getProperty("clean.speakertags",
	                              CleanXmlAnnotator.DEFAULT_SPEAKER_TAGS);
	              String docAnnotations =
	                      properties.getProperty("clean.docAnnotations",
	                              CleanXmlAnnotator.DEFAULT_DOC_ANNOTATIONS_PATTERNS);
	              String tokenAnnotations =
	                      properties.getProperty("clean.tokenAnnotations",
	                              CleanXmlAnnotator.DEFAULT_TOKEN_ANNOTATIONS_PATTERNS);
	              String sectionTags =
	                      properties.getProperty("clean.sectiontags",
	                              CleanXmlAnnotator.DEFAULT_SECTION_TAGS);
	              String sectionAnnotations =
	                      properties.getProperty("clean.sectionAnnotations",
	                              CleanXmlAnnotator.DEFAULT_SECTION_ANNOTATIONS_PATTERNS);
	              String ssplitDiscardTokens =
	                      properties.getProperty("clean.ssplitDiscardTokens");
	              CleanXmlAnnotator annotator = new CleanXmlAnnotator(xmlTags,
	                  sentenceEndingTags,
	                  dateTags,
	                  allowFlawed);
	              annotator.setSingleSentenceTagMatcher(singleSentenceTags);
	              annotator.setDocIdTagMatcher(docIdTags);
	              annotator.setDocTypeTagMatcher(docTypeTags);
	              annotator.setDiscourseTags(utteranceTurnTags, speakerTags);
	              annotator.setDocAnnotationPatterns(docAnnotations);
	              annotator.setTokenAnnotationPatterns(tokenAnnotations);
	              annotator.setSectionTagMatcher(sectionTags);
	              annotator.setSectionAnnotationPatterns(sectionAnnotations);
	              annotator.setSsplitDiscardTokensMatcher(ssplitDiscardTokens);
	              return annotator;
	            }

	            @Override
	            public String signature() {
	              // keep track of all relevant properties for this annotator here!
	              return "clean.xmltags:" +
	                      properties.getProperty("clean.xmltags",
	                        CleanXmlAnnotator.DEFAULT_XML_TAGS) +
	                      "clean.sentenceendingtags:" +
	                      properties.getProperty("clean.sentenceendingtags",
	                        CleanXmlAnnotator.DEFAULT_SENTENCE_ENDERS) +
	                      "clean.sentenceendingtags:" +
	                      properties.getProperty("clean.singlesentencetags",
	                              CleanXmlAnnotator.DEFAULT_SINGLE_SENTENCE_TAGS) +
	                      "clean.allowflawedxml:" +
	                      properties.getProperty("clean.allowflawedxml", "") +
	                      "clean.datetags:" +
	                      properties.getProperty("clean.datetags",
	                        CleanXmlAnnotator.DEFAULT_DATE_TAGS) +
	                      "clean.docidtags:" +
	                      properties.getProperty("clean.docid",
	                              CleanXmlAnnotator.DEFAULT_DOCID_TAGS) +
	                      "clean.doctypetags:" +
	                      properties.getProperty("clean.doctype",
	                              CleanXmlAnnotator.DEFAULT_DOCTYPE_TAGS) +
	                      "clean.turntags:" +
	                      properties.getProperty("clean.turntags",
	                        CleanXmlAnnotator.DEFAULT_UTTERANCE_TURN_TAGS) +
	                      "clean.speakertags:" +
	                      properties.getProperty("clean.speakertags",
	                        CleanXmlAnnotator.DEFAULT_SPEAKER_TAGS) +
	                      "clean.docAnnotations:" +
	                      properties.getProperty("clean.docAnnotations",
	                        CleanXmlAnnotator.DEFAULT_DOC_ANNOTATIONS_PATTERNS) +
	                      "clean.tokenAnnotations:" +
	                      properties.getProperty("clean.tokenAnnotations",
	                              CleanXmlAnnotator.DEFAULT_TOKEN_ANNOTATIONS_PATTERNS) +
	                      "clean.sectiontags:" +
	                      properties.getProperty("clean.sectiontags",
	                        CleanXmlAnnotator.DEFAULT_SECTION_TAGS) +
	                      "clean.sectionAnnotations:" +
	                      properties.getProperty("clean.sectionAnnotations",
	                              CleanXmlAnnotator.DEFAULT_SECTION_ANNOTATIONS_PATTERNS);
	            }
	          });
	    }

	    
	    if (STANFORD_SSPLIT.equals(annotatorName)) {
	    
	          //
	          // sentence splitter: splits the above sequence of tokens into
	          // sentences.  This is required when processing entire documents or
	          // text consisting of multiple sentences
	          //
	        pool.register(STANFORD_SSPLIT, new AnnotatorFactory(inputProps) {
	            private static final long serialVersionUID = 1L;
	            @Override
	            public Annotator create() {
	              System.err.println(signature());
	              boolean nlSplitting = Boolean.valueOf(properties.getProperty(NEWLINE_SPLITTER_PROPERTY, "false"));
	              if (nlSplitting) {
	                boolean whitespaceTokenization = Boolean.valueOf(properties.getProperty("tokenize.whitespace", "false"));
	                if (whitespaceTokenization) {
	                  if (System.getProperty("line.separator").equals("\n")) {
	                    return WordsToSentencesAnnotator.newlineSplitter(false, "\n");
	                  } else {
	                    // throw "\n" in just in case files use that instead of
	                    // the system separator
	                    return WordsToSentencesAnnotator.newlineSplitter(false, System.getProperty("line.separator"), "\n");
	                  }
	                } else {
	                  return WordsToSentencesAnnotator.newlineSplitter(false, PTBTokenizer.getNewlineToken());
	                }

	              } else {
	                // Treat as one sentence: You get a no-op sentence splitter that always returns all tokens as one sentence.
	                String isOneSentence = properties.getProperty("ssplit.isOneSentence");
	                if (Boolean.parseBoolean(isOneSentence)) { // this method treats null as false
	                  return WordsToSentencesAnnotator.nonSplitter(false);
	                }

	                // multi token sentence boundaries
	                String boundaryMultiTokenRegex = properties.getProperty("ssplit.boundaryMultiTokenRegex");

	                // Discard these tokens without marking them as sentence boundaries
	                String tokenPatternsToDiscardProp = properties.getProperty("ssplit.tokenPatternsToDiscard");
	                Set<String> tokenRegexesToDiscard = null;
	                if (tokenPatternsToDiscardProp != null){
	                  String [] toks = tokenPatternsToDiscardProp.split(",");
	                  tokenRegexesToDiscard = Generics.newHashSet(Arrays.asList(toks));
	                }
	                // regular boundaries
	                String boundaryTokenRegex = properties.getProperty("ssplit.boundaryTokenRegex");
	                Set<String> boundariesToDiscard = null;

	                // newline boundaries which are discarded.
	                String bounds = properties.getProperty("ssplit.boundariesToDiscard");
	                if (bounds != null) {
	                  String [] toks = bounds.split(",");
	                  boundariesToDiscard = Generics.newHashSet(Arrays.asList(toks));
	                }
	                Set<String> htmlElementsToDiscard = null;
	                // HTML boundaries which are discarded
	                bounds = properties.getProperty("ssplit.htmlBoundariesToDiscard");
	                if (bounds != null) {
	                  String [] elements = bounds.split(",");
	                  htmlElementsToDiscard = Generics.newHashSet(Arrays.asList(elements));
	                }
	                String nlsb = properties.getProperty(NEWLINE_IS_SENTENCE_BREAK_PROPERTY, DEFAULT_NEWLINE_IS_SENTENCE_BREAK);

	                return new WordsToSentencesAnnotator(false, boundaryTokenRegex, boundariesToDiscard, htmlElementsToDiscard,
	                        nlsb, boundaryMultiTokenRegex, tokenRegexesToDiscard);
	              }
	            }

	            @Override
	            public String signature() {
	              // keep track of all relevant properties for this annotator here!
	              StringBuilder os = new StringBuilder();
	              if (Boolean.valueOf(properties.getProperty(NEWLINE_SPLITTER_PROPERTY, "false"))) {
	                os.append(NEWLINE_SPLITTER_PROPERTY + "=" + properties.getProperty(NEWLINE_SPLITTER_PROPERTY, "false") + "\n");
	                os.append("tokenize.whitespace=" + properties.getProperty("tokenize.whitespace", "false") + "\n");
	              } else {
	                os.append(baseSignature(properties, STANFORD_SSPLIT));
	              }
	              return os.toString();
	            }
	          });

	    }
	    
	    if (STANFORD_POS.equals(annotatorName)) {
	          //
	          // POS tagger
	          //
	        pool.register(STANFORD_POS, new AnnotatorFactory(inputProps) {
	            private static final long serialVersionUID = 1L;
	            @Override
	            public Annotator create() {
	              try {
	                return new POSTaggerAnnotator("pos", properties);
	              } catch (Exception e) {
	                throw new RuntimeException(e);
	              }
	            }

	            @Override
	            public String signature() {
	              // keep track of all relevant properties for this annotator here!
	              return POSTaggerAnnotator.signature(properties);
	            }
	          });
	     }
	    
	    if (STANFORD_LEMMA.equals(annotatorName)) {
	          //
	          // Lemmatizer
	          //
	        pool.register(STANFORD_LEMMA, new AnnotatorFactory(inputProps) {
	            private static final long serialVersionUID = 1L;
	            @Override
	            public Annotator create() {
	              return new MorphaAnnotator(false);
	            }

	            @Override
	            public String signature() {
	              // keep track of all relevant properties for this annotator here!
	              // nothing for this one
	              return "";
	            }
	          });
	    }

	    if (STANFORD_NER.equals(annotatorName)) {
	          //
	          // NER
	          //
	        pool.register(STANFORD_NER, new AnnotatorFactory(inputProps) {
	            private static final long serialVersionUID = 1L;
	            @Override
	            public Annotator create() {
	              List<String> models = new ArrayList<String>();
	              String modelNames = properties.getProperty("ner.model");
	              if (modelNames == null) {
	                modelNames = DefaultPaths.DEFAULT_NER_THREECLASS_MODEL + "," + DefaultPaths.DEFAULT_NER_MUC_MODEL + "," + DefaultPaths.DEFAULT_NER_CONLL_MODEL;
	              }
	              if (modelNames.length() > 0) {
	                models.addAll(Arrays.asList(modelNames.split(",")));
	              }
	              if (models.isEmpty()) {
	                // Allow for no real NER model - can just use numeric classifiers or SUTime.
	                // Have to unset ner.model, so unlikely that people got here by accident.
	                System.err.println("WARNING: no NER models specified");
	              }
	              NERClassifierCombiner nerCombiner;
	              try {
	                boolean applyNumericClassifiers =
	                  PropertiesUtils.getBool(properties,
	                      NERClassifierCombiner.APPLY_NUMERIC_CLASSIFIERS_PROPERTY,
	                      NERClassifierCombiner.APPLY_NUMERIC_CLASSIFIERS_DEFAULT);
	                boolean useSUTime =
	                  PropertiesUtils.getBool(properties,
	                      NumberSequenceClassifier.USE_SUTIME_PROPERTY,
	                      NumberSequenceClassifier.USE_SUTIME_DEFAULT);
	                nerCombiner = new NERClassifierCombiner(applyNumericClassifiers,
	                      useSUTime, properties,
	                      models.toArray(new String[models.size()]));
	              } catch (FileNotFoundException e) {
	                throw new RuntimeIOException(e);
	              }
	              return new NERCombinerAnnotator(nerCombiner, false);
	            }

	            @Override
	            public String signature() {
	              // keep track of all relevant properties for this annotator here!
	              return "ner.model:" +
	                      properties.getProperty("ner.model", "") +
	                      "ner.model.3class:" +
	                      properties.getProperty("ner.model.3class",
	                              DefaultPaths.DEFAULT_NER_THREECLASS_MODEL) +
	                      "ner.model.7class:" +
	                      properties.getProperty("ner.model.7class",
	                              DefaultPaths.DEFAULT_NER_MUC_MODEL) +
	                      "ner.model.MISCclass:" +
	                      properties.getProperty("ner.model.MISCclass",
	                              DefaultPaths.DEFAULT_NER_CONLL_MODEL) +
	                      NERClassifierCombiner.APPLY_NUMERIC_CLASSIFIERS_PROPERTY + ":" +
	                      properties.getProperty(NERClassifierCombiner.APPLY_NUMERIC_CLASSIFIERS_PROPERTY,
	                              Boolean.toString(NERClassifierCombiner.APPLY_NUMERIC_CLASSIFIERS_DEFAULT)) +
	                      NumberSequenceClassifier.USE_SUTIME_PROPERTY + ":" +
	                      properties.getProperty(NumberSequenceClassifier.USE_SUTIME_PROPERTY,
	                              Boolean.toString(NumberSequenceClassifier.USE_SUTIME_DEFAULT));
	            }
	          });
	    }
	    
	    if (STANFORD_REGEXNER.equals(annotatorName)) {
	          //
	          // Regex NER
	          //
	        pool.register(STANFORD_REGEXNER, new AnnotatorFactory(inputProps) {
	            private static final long serialVersionUID = 1L;
	            @Override
	            public Annotator create() {
	              return new TokensRegexNERAnnotator("regexner", properties);
	            }

	            @Override
	            public String signature() {
	              // keep track of all relevant properties for this annotator here!
	              return PropertiesUtils.getSignature("regexner", properties, TokensRegexNERAnnotator.SUPPORTED_PROPERTIES);
	            }
	          });
	    }

	    if (STANFORD_GENDER.equals(annotatorName)) {
	          //
	          // Gender Annotator
	          //
	        pool.register(STANFORD_GENDER, new AnnotatorFactory(inputProps) {
	            private static final long serialVersionUID = 1L;
	            @Override
	            public Annotator create() {
	              return new GenderAnnotator(false, properties.getProperty("gender.firstnames", DefaultPaths.DEFAULT_GENDER_FIRST_NAMES));
	            }

	            @Override
	            public String signature() {
	              // keep track of all relevant properties for this annotator here!
	              return "gender.firstnames:" +
	                      properties.getProperty("gender.firstnames",
	                              DefaultPaths.DEFAULT_GENDER_FIRST_NAMES);
	            }
	          });
	    }

	    if (STANFORD_TRUECASE.equals(annotatorName)) {
	          //
	          // True caser
	          //
	        pool.register(STANFORD_TRUECASE, new AnnotatorFactory(inputProps) {
	            private static final long serialVersionUID = 1L;
	            @Override
	            public Annotator create() {
	              String model = properties.getProperty("truecase.model", DefaultPaths.DEFAULT_TRUECASE_MODEL);
	              String bias = properties.getProperty("truecase.bias", TrueCaseAnnotator.DEFAULT_MODEL_BIAS);
	              String mixed = properties.getProperty("truecase.mixedcasefile", DefaultPaths.DEFAULT_TRUECASE_DISAMBIGUATION_LIST);
	              return new TrueCaseAnnotator(model, bias, mixed, false);
	            }

	            @Override
	            public String signature() {
	              // keep track of all relevant properties for this annotator here!
	              return "truecase.model:" +
	                      properties.getProperty("truecase.model",
	                              DefaultPaths.DEFAULT_TRUECASE_MODEL) +
	                      "truecase.bias:" +
	                      properties.getProperty("truecase.bias",
	                              TrueCaseAnnotator.DEFAULT_MODEL_BIAS) +
	                      "truecase.mixedcasefile:" +
	                      properties.getProperty("truecase.mixedcasefile",
	                              DefaultPaths.DEFAULT_TRUECASE_DISAMBIGUATION_LIST);
	            }
	          });
	    }
	    
//	    if (STANFORD_NFL_TOKENIZE.equals(annotatorName)) {
//	          //
//	          // Post-processing tokenization rules for the NFL domain
//	          //
//	          pool.register(STANFORD_NFL_TOKENIZE, new AnnotatorFactory(inputProps) {
//	            private static final long serialVersionUID = 1L;
//	            @Override
//	            public Annotator create() {
//	              final String className =
//	                "edu.stanford.nlp.pipeline.NFLTokenizerAnnotator";
//	              return ReflectionLoading.loadByReflection(className);
//	            }
//
//	            @Override
//	            public String signature() {
//	              // keep track of all relevant properties for this annotator here!
//	              StringBuilder os = new StringBuilder();
//	              // no used props for this one
//	              return os.toString();
//	            }
//	          });
//	    }

//	    if (STANFORD_NFL.equals(annotatorName)) {
//	          //
//	          // Entity and relation extraction for the NFL domain
//	          //
//	          pool.register(STANFORD_NFL, new AnnotatorFactory(inputProps) {
//	            private static final long serialVersionUID = 1L;
//	            @Override
//	            public Annotator create() {
//	              // these paths now extracted inside c'tor
//	              // String gazetteer = properties.getProperty("nfl.gazetteer", DefaultPaths.DEFAULT_NFL_GAZETTEER);
//	              // String entityModel = properties.getProperty("nfl.entity.model", DefaultPaths.DEFAULT_NFL_ENTITY_MODEL);
//	              // String relationModel = properties.getProperty("nfl.relation.model", DefaultPaths.DEFAULT_NFL_RELATION_MODEL);
//	              final String className = "edu.stanford.nlp.pipeline.NFLAnnotator";
//	              return ReflectionLoading.loadByReflection(className, properties);
//	            }
//
//	            @Override
//	            public String signature() {
//	              // keep track of all relevant properties for this annotator here!
//	              StringBuilder os = new StringBuilder();
//	              os.append("nfl.verbose:" +
//	                      properties.getProperty("nfl.verbose",
//	                              "false"));
//	              os.append("nfl.relations.use.max.recall:" +
//	                      properties.getProperty("nfl.relations.use.max.recall",
//	                              "false"));
//	              os.append("nfl.relations.use.model.merging:" +
//	                      properties.getProperty("nfl.relations.use.model.merging",
//	                              "false"));
//	              os.append("nfl.relations.use.basic.inference:" +
//	                      properties.getProperty("nfl.relations.use.basic.inference",
//	                              "true"));
//	              os.append("nfl.gazetteer:" +
//	                      properties.getProperty("nfl.gazetteer",
//	                              DefaultPaths.DEFAULT_NFL_GAZETTEER));
//	              os.append("nfl.entity.model:" +
//	                      properties.getProperty("nfl.entity.model",
//	                              DefaultPaths.DEFAULT_NFL_ENTITY_MODEL));
//	              os.append("nfl.relation.model:" +
//	                      properties.getProperty("nfl.relation.model",
//	                              DefaultPaths.DEFAULT_NFL_RELATION_MODEL));
//	              return os.toString();
//	            }
//	          });
//	    }

	    if (STANFORD_PARSE.equals(annotatorName)) {
	          //
	          // Parser
	          //
	        pool.register(STANFORD_PARSE, new AnnotatorFactory(inputProps) {
	            private static final long serialVersionUID = 1L;
	            @Override
	            public Annotator create() {
	              String parserType = properties.getProperty("parse.type", "stanford");
	              String maxLenStr = properties.getProperty("parse.maxlen");

	              if (parserType.equalsIgnoreCase("stanford")) {
	                ParserAnnotator anno = new ParserAnnotator("parse", properties);
	                return anno;
	              } else if (parserType.equalsIgnoreCase("charniak")) {
	                String model = properties.getProperty("parse.model");
	                String parserExecutable = properties.getProperty("parse.executable");
	                if (model == null || parserExecutable == null) {
	                  throw new RuntimeException("Both parse.model and parse.executable properties must be specified if parse.type=charniak");
	                }
	                int maxLen = 399;
	                if (maxLenStr != null) {
	                  maxLen = Integer.parseInt(maxLenStr);
	                }

	                CharniakParserAnnotator anno = new CharniakParserAnnotator(model, parserExecutable, false, maxLen);

	                return anno;
	              } else {
	                throw new RuntimeException("Unknown parser type: " + parserType + " (currently supported: stanford and charniak)");
	              }
	            }

	            @Override
	            public String signature() {
	              // keep track of all relevant properties for this annotator here!
	              String type = properties.getProperty("parse.type", "stanford");
	              if(type.equalsIgnoreCase("stanford")){
	                return ParserAnnotator.signature("parse", properties);
	              } else if(type.equalsIgnoreCase("charniak")) {
	                return "parse.model:" +
	                        properties.getProperty("parse.model", "") +
	                        "parse.executable:" +
	                        properties.getProperty("parse.executable", "") +
	                        "parse.maxlen:" +
	                        properties.getProperty("parse.maxlen", "");
	              } else {
	                throw new RuntimeException("Unknown parser type: " + type +
	                        " (currently supported: stanford and charniak)");
	              }
	            }
	          });
	    }

	    if (STANFORD_DETERMINISTIC_COREF.equals(annotatorName)) {
	          //
	          // Coreference resolution
	          //
	          pool.register(STANFORD_DETERMINISTIC_COREF, new AnnotatorFactory(inputProps) {
	            private static final long serialVersionUID = 1L;
	            @Override
	            public Annotator create() {
	              return new DeterministicCorefAnnotator(properties);
	            }

	            @Override
	            public String signature() {
	              // keep track of all relevant properties for this annotator here!
	              return DeterministicCorefAnnotator.signature(properties);
	            }
	          });
	    }
	          
	    // add annotators loaded via reflection from classnames specified
	    // in the properties
	    for (String property : inputProps.stringPropertyNames()) {
	      if (property.startsWith(CUSTOM_ANNOTATOR_PREFIX)) {
	        final String customName =
	          property.substring(CUSTOM_ANNOTATOR_PREFIX.length());
	        final String customClassName = inputProps.getProperty(property);
	        System.err.println("Registering annotator " + customName +
	            " with class " + customClassName);
	        pool.register(customName, new AnnotatorFactory(inputProps) {
	          private static final long serialVersionUID = 1L;
	          private final String name = customName;
	          private final String className = customClassName;
	          @Override
	          public Annotator create() {
	            return ReflectionLoading.loadByReflection(className, name,
	                                                      properties);
	          }
	          @Override
	          public String signature() {
	            // keep track of all relevant properties for this annotator here!
	            // since we don't know what props they need, let's copy all
	            // TODO: can we do better here? maybe signature() should be a method in the Annotator?
	            StringBuilder os = new StringBuilder();
	            for(Object key: properties.keySet()) {
	              String skey = (String) key;
	              os.append(skey + ":" + properties.getProperty(skey));
	            }
	            return os.toString();
	          }
	        });
	      }
	    }


	    pool.register(STANFORD_RELATION, new AnnotatorFactory(inputProps) {
	      private static final long serialVersionUID = 1L;
	      @Override
	      public Annotator create() {
	        return new RelationExtractorAnnotator(properties);
	      }

	      @Override
	      public String signature() {
	        // keep track of all relevant properties for this annotator here!
	        return "sup.relation.verbose:" +
	        properties.getProperty("sup.relation.verbose",
	                "false") +
	        properties.getProperty("sup.relation.model",
	                DefaultPaths.DEFAULT_SUP_RELATION_EX_RELATION_MODEL);
	      }
	    });

	    pool.register(STANFORD_SENTIMENT, new AnnotatorFactory(inputProps) {
	      private static final long serialVersionUID = 1L;
	      @Override
	      public Annotator create() {
	        return new SentimentAnnotator(STANFORD_SENTIMENT, properties);
	      }

	      @Override
	      public String signature() {
	        return "sentiment.model=" + inputProps.get("sentiment.model");
	      }
	    });	    
	}
}
