//package com.readr.spark.malt;
//
//import java.io.File;
//import java.net.URI;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Properties;
//
//import org.maltparser.MaltParserService;
//import org.maltparser.core.exception.MaltChainedException;
//import org.maltparser.core.symbol.SymbolTable;
//import org.maltparser.core.syntaxgraph.DependencyStructure;
//import org.maltparser.core.syntaxgraph.edge.Edge;
//import org.maltparser.core.syntaxgraph.node.TokenNode;
//
//import com.readr.protobuf.dependency.DependencyProto;
//import com.readr.protobuf.pos.POSProto;
//import com.readr.protobuf.sentence.SentenceProto;
//import com.readr.protobuf.token.TokenProto;
//import com.readr.protobuf.token.TokenProto.Token;
//import com.readr.hadoop.common.AbstractAnnotator;
//
//// java -jar maltparser-1.7.2.jar -c test -i examples/data/talbanken05_test.conll -o out.conll -m parse
//
//// not sure if useful, but this was used on pardosa07
//// java -Xmx1024m -jar malt.jar -w tmp30 -c engmalt -ic utf-8 -i /projects/pardosa/data14/raphaelh/riedel0506/recreate/t3/x00.in -oc utf-8 -of conllx -o /projects/pardosa/data14/raphaelh/riedel0506/recreate/t3/x00.out -m parse &
//
//public class MaltparserDependencyAnnotator extends AbstractAnnotator {
//	static String GENERATES = "DepMalt";
//	static String REQUIRES = "Token,Sentence,POS";
//
//	static String modelFile = "engmalt.linear-1.7.mco";
//	static String workingDir = ".";
//
//	static String readrTmpDir;
//	
//	public MaltparserDependencyAnnotator(Properties properties) {
//		super(properties);
//		init(GENERATES, REQUIRES);
//
//		
//		String tmpDir = properties.getProperty("tmpDir", "/tmp");
//		readrTmpDir = tmpDir + "/readr-data_" + System.currentTimeMillis();
//		//readrTmpDir = tmpDir;
//	}
//
//	private MaltParserService service;
//
//	public void init() throws Exception {
//		// unpack
//		
//		File tmpDirFile = new File(readrTmpDir);
//		//if (tmpDirFile.exists()) tmpDirFile.delete();
//		
//		if (!tmpDirFile.exists()) {
//			tmpDirFile.mkdirs();
//			URI jarURI = com.readr.hadoop.util.FileUtils.getJarFromClass(MaltparserDependencyAnnotator.class);
//			System.out.println("now extracting");
//			com.readr.hadoop.util.FileUtils.extractFromZip(jarURI, "engmalt.linear-1.7.mco", readrTmpDir);
//		}
//		
//		service = new MaltParserService();
//		// Inititalize the parser model 'model0' and sets the working
//		// directory to '.' and sets the logging file to 'parser.log'
//
//		workingDir = readrTmpDir;
//		service.initializeParserModel("-w " + workingDir + " -c "
//				+ modelFile + " -m parse");
//		// service.initializeParserModel("-c model0 -m parse -w . -lfi parser.log");
//		// parser.initializeParserModel("-u " + modelUrl.toString() +
//		// " -m parse");
//	}
//
//	public Object annotate(Object... ins) {
//		TokenProto.Doc tp = (TokenProto.Doc)ins[0];
//		SentenceProto.Doc sp = (SentenceProto.Doc)ins[1];
//		POSProto.Doc pp = (POSProto.Doc)ins[2];
//		DependencyProto.Doc dp = run(tp, sp, pp);
//		return dp;
//	}
//
//	private DependencyProto.Doc run(TokenProto.Doc tp, SentenceProto.Doc sp, POSProto.Doc pp) {
//		List<DependencyProto.Sen> sps = new ArrayList<DependencyProto.Sen>();
//		for (int sen = 0; sen < sp.getSentencesCount(); sen++) {
//			SentenceProto.Sen spSen = sp.getSentences(sen);
//
//			// this step is necessary to report progress to hadoop,
//			// so that it doesn't kill this task if there is a document
//			// with very many sentences
//			if (reporter != null)
//				reporter.incrementCounter();
//
//			List<Token> toks = tp.getTokensList().subList(
//					spSen.getTokenBegin(), spSen.getTokenEnd());
//			List<String> poss = pp.getPosList().subList(spSen.getTokenBegin(),
//					spSen.getTokenEnd());
//
//			List<DependencyProto.Dep> dps = new ArrayList<DependencyProto.Dep>();
//
//			try {
//				// Creates an array of tokens, which contains the Swedish
//				// sentence 'Grundavdraget upph??r allts?? vid en taxerad inkomst
//				// p?? 52500 kr.'
//				// in the CoNLL data format.
//				String[] parserInput = new String[toks.size()];
//				for (int i = 0; i < toks.size(); i++) {
//					Token t = toks.get(i);
//					String pos = poss.get(i);
//					parserInput[i] = (i + 1) + "\t" + t.getOriginal() + "\t"
//							+ "_" + "\t" + pos; // + "\"
//				}
//
//				// Run parser
//				DependencyStructure outputGraph = null;
//				SymbolTable symbolTable;
//				try {
//					outputGraph = service.parse(parserInput);
//					symbolTable = outputGraph.getSymbolTables().getSymbolTable(
//							"DEPREL");
//				} catch (MaltChainedException e) {
//					// logger.log(Level.WARNING,
//					// "MaltParser exception while parsing sentence: " +
//					// e.getMessage(), e);
//					// don't pass on exception - go on with next sentence
//					continue;
//				}
//				for (int i = 0; i < toks.size(); i++) {
//					TokenNode node = outputGraph.getTokenNode(i + 1);
//					for (Edge edge : node.getHeadEdges()) {
//						int from = edge.getSource().getIndex() - 1;
//						int to = edge.getTarget().getIndex() - 1;
//
//						DependencyProto.Dep dp = DependencyProto.Dep
//								.newBuilder().setHead(from).setDependent(to)
//								.setRelation(edge.getLabelSymbol(symbolTable))
//								.build();
//						dps.add(dp);
//					}
//				}
//			} catch (MaltChainedException e) {
//				System.err.println("MaltParser exception: " + e.getMessage());
//			}
//			sps.add(DependencyProto.Sen.newBuilder()
//					.addAllDependencies(dps).build());
//		}
//		return DependencyProto.Doc.newBuilder().addAllSentences(sps).build();
//	}
//	
//	public void close() throws Exception {
//		service.terminateParserModel();		
//	}
//}
