//
//
//import java.io.BufferedReader;
//import java.io.File;
//import java.io.InputStream;
//import java.io.InputStreamReader;
//import java.io.OutputStream;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Properties;
//import java.util.UUID;
//import java.util.concurrent.LinkedBlockingQueue;
//import java.util.concurrent.TimeUnit;
//
//import com.readr.protobuf.parse.ParseProto;
//import com.readr.protobuf.sentence.SentenceProto;
//import com.readr.protobuf.sentence.SentenceProto.Sen;
//import com.readr.protobuf.token.TokenProto;
//import com.readr.protobuf.token.TokenProto.Token;
//import com.readr.hadoop.common.AbstractAnnotator;
//
//// note: to compile the cj parser on some platforms, one needs to add #include <getopt.h>
//// to second-stage/features/best-parses.cc
//
//// If you compile this on Mac, make sure you use the gcc compiler, not clang which is default
//// with macports you can switch
////   sudo port install gcc_select
////   sudo port select --list gcc
////   sudo port select gcc llvm-gcc42
////   readlink `which c++`
//
//
//// test, using:
////cat steedman.txt | first-stage/PARSE/parseIt -l399 -N50 -K first-stage/DATA/EN/ | second-stage/programs/features/best-parses -l second-stage/models/ec50spfinal/features.gz second-stage/models/ec50spfinal/cvlm-l1c10P1-weights.gz
//
//public class CJParseAnnotator extends AbstractAnnotator {
//
//	static String GENERATES = "Parse";
//	static String REQUIRES = "Token,Sentence";
//
//	//static String dir = "/work/soft/cj/reranking-parser";
//	static String dir = "/tmp/cj/reranking-parser";
//	static String cmd1 = "first-stage/PARSE/parseIt";
//	static String cmd2 = "second-stage/programs/features/best-parses";
//	static String modelDir = "second-stage/models/ec50spfinal";
//	static String estimatorNickname = "cvlm-l1c10P1";
//	
//	private ProcessBuilder pb1, pb2;
//	private Process p1, p2;
//	private Piper pipe;
//	private InputPiper inputQueuePipe;
//	private OutputQueuePiper outputQueuePipe;
//	private OutputPrintPiper error1Pipe, error2Pipe;
//	private String readrTmpDir;
//	
//	public CJParseAnnotator(Properties properties) throws Exception {
//		super(properties);
//		init(GENERATES, REQUIRES);
//		
//		// if tmp cj dir does not exist, then unpack cj from resource zip first
//		String tmpDir = properties.getProperty("tmpDir", "/tmp");
//		//readrTmpDir = tmpDir + "/readr-data_" + System.currentTimeMillis();
//		//readrTmpDir = tmpDir;
//		readrTmpDir = "/tmp";
//		System.out.println("tmpDir: " + readrTmpDir);		
//	}
//	
//	public void init() throws Exception {
//		File tmpDirFile = new File(readrTmpDir);
//		//if (tmpDirFile.exists()) tmpDirFile.delete();
//
//		String dir = readrTmpDir + "/cj/reranking-parser";
//
//		if (!new File(dir).exists()) {
//			tmpDirFile.mkdirs();
//			
//			//URI jarURI2 = CJParseAnnotator.class.getProtectionDomain().getCodeSource().getLocation().toURI();
//			System.out.println("extracting cj.zip");
//			InputStream is = CJParseAnnotator.class.getResourceAsStream("cj.zip");
//			com.readr.hadoop.util.FileUtils.dumpStreamToFile(is, "cj.zip", readrTmpDir);
//			
//			//URI jarURI = url.toURI();
//			//System.out.println(jarURI.toString());
//			//URI jarURI = com.readr.process.util.FileUtils.getJarFromClass(CJParseAnnotator.class);
//			//System.out.println("now extracting");
//			//com.readr.process.util.FileUtils.extractFromZip(jarURI, "cj.zip", readrTmpDir);
//			com.readr.hadoop.util.FileUtils.extractZipFile(readrTmpDir + "/" + "cj.zip", readrTmpDir);
//			for (String s : tmpDirFile.list())
//				System.out.println("   file: " + s);
//			
//			// chmod
//			// java 6
//			new File(readrTmpDir + "/cj/reranking-parser" + "/" + cmd1).setExecutable(true);
//			new File(readrTmpDir + "/cj/reranking-parser" + "/" + cmd2).setExecutable(true);
//
//			// the following is Java 7 only
////			java.nio.file.Files.setPosixFilePermissions(
////					FileSystems.getDefault().getPath(readrTmpDir + "/cj/reranking-parser", cmd1),
////					PosixFilePermissions.fromString("rwxr-x---"));
////			java.nio.file.Files.setPosixFilePermissions(
////					FileSystems.getDefault().getPath(readrTmpDir + "/cj/reranking-parser", cmd2),
////					PosixFilePermissions.fromString("rwxr-x---"));
//		}
//		
//		pb1 = new ProcessBuilder(cmd1, "-l399", "-N50", "-K", "first-stage/DATA/EN/");
//		pb2 = new ProcessBuilder(cmd2, "-l", modelDir + "/features.gz", 
//						modelDir + "/" + estimatorNickname + "-weights.gz");
//
//		pb1.directory(new File(dir));
//		pb2.directory(new File(dir));
//		
//		reset();
//	}
//	
//	private void reset() throws Exception {
//		// sometimes I get the following error at a job begin here:
//		// java.io.IOException: Cannot run program "first-stage/PARSE/parseIt" (in directory "/mnt2/var/lib/hadoop/mapred/taskTracker/hadoop/jobcache/job_201302142005_0001/work/readr-data_1360872723154/cj/reranking-parser"): java.io.IOException: error=2, No such file or directory
//				
//		p1 = pb1.start();
//		p2 = pb2.start();
//		
//		pipe = new Piper(p1.getInputStream(), p2.getOutputStream());
//		inputQueuePipe = new InputPiper(p1.getOutputStream());
//		outputQueuePipe = new OutputQueuePiper(p2.getInputStream());
//		error1Pipe = new OutputPrintPiper(p1.getErrorStream());
//		error2Pipe = new OutputPrintPiper(p2.getErrorStream());
//		
//		new Thread(pipe).start();
//		new Thread(outputQueuePipe).start();
//		new Thread(inputQueuePipe).start();
//		new Thread(error1Pipe).start();
//		new Thread(error2Pipe).start();
//	}
//	
//	public Object annotate(Object... ins) {
//		TokenProto.Doc tp = (TokenProto.Doc)ins[0];
//		SentenceProto.Doc sp = (SentenceProto.Doc)ins[1];
//		ParseProto.Doc pp = run(tp, sp);
//		return pp;
//	}
//
//	private ParseProto.Doc run(TokenProto.Doc tp, SentenceProto.Doc sp) {		
//		List<ParseProto.Sen> ppsens = new ArrayList<ParseProto.Sen>();
//		for (Sen sen : sp.getSentencesList()) {
//			// this step is necessary to report progress to hadoop,
//			// so that it doesn't kill this task if there is a document
//			// with very many sentences
//			if (reporter != null) reporter.incrementCounter();
//			
//			List<Token> toks = tp.getTokensList().subList(sen.getTokenBegin(), sen.getTokenEnd());
//						
//			StringBuilder sb = new StringBuilder();
//			sb.append("<s>");
//			for (Token tok : toks)
//				sb.append(" " + tok.getValue());
//			sb.append(" </s>\n");
//			String inputSentence = sb.toString();
//			//String inputSentence = "<s> This cat likes fresh fish. </s>\n";
//
//			// send to parser
//			try {
//				inputQueuePipe.next(inputSentence);
//			} catch (Exception e1) {
//				e1.printStackTrace();
//			}
//			
//			// wait for output
//			String tree = outputQueuePipe.next();
//			if (tree == null) {				
//				try {
//					System.out.println("RESET");
//					close();
//					reset();
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
//				//tree = "";
//			}
//			//trees.add(tree);
//			
//			ParseProto.Sen ppsen;
//			if (tree != null)
//				ppsen = ParseProto.Sen.newBuilder().setSentNum(sen.getSentNum()).setTree(tree).build();
//			else
//				ppsen = ParseProto.Sen.newBuilder().setSentNum(sen.getSentNum()).build();
//			ppsens.add(ppsen);
//			
//			System.out.println("Tree for: " + inputSentence);
//			System.out.println(tree);
//			//System.out.println(sop.getFile() + "\t" + sentNum);
//		}
//		ParseProto.Doc pp = ParseProto.Doc.newBuilder()
//				.addAllSentences(ppsens).setId(tp.getId()).build();
//		return pp;
//	}
//	
//	public void close() throws Exception {
//		// destroy external processes
//		if (p1 != null)
//			try { p1.destroy(); } catch (Exception e) {}
//		if (p2 != null)
//			try { p2.destroy(); } catch (Exception e) {}
//
//		// close all piping threads
//		if (pipe != null)
//			pipe.close();
//		if (inputQueuePipe != null)
//			inputQueuePipe.close();
//		if (outputQueuePipe != null)
//			outputQueuePipe.close();
//		if (error1Pipe != null)
//			error1Pipe.close();
//		if (error2Pipe != null)
//			error2Pipe.close();
//	}
//	
//	// connects and input stream to an output stream 
//	private static class Piper implements java.lang.Runnable {
//		   private java.io.InputStream input;
//
//		    private java.io.OutputStream output;
//
//		    public Piper(java.io.InputStream input, java.io.OutputStream output) {
//		        this.input = input;
//		        this.output = output;
//		    }
//
//		    public void run() {
//		        try {
//		            byte[] b = new byte[512];
//		            int read = 1;
//		            while ((read = input.read(b, 0, b.length)) > -1) {
//		            	output.write(b, 0, read);
//		            	output.flush();
//		            }
//		        } catch (Exception e) {
//		            throw new RuntimeException("Broken pipe", e);
//		        } finally {
//		            try {
//		                input.close();
//		            } catch (Exception e) {
//		            }
//		            try {
//		                output.close();
//		            } catch (Exception e) {
//		            }
//		        }
//		    }
//		    
//		    public void close() {
//	            try {
//	                input.close();
//	            } catch (Exception e) {
//	            }
//	            try {
//	                output.close();
//	            } catch (Exception e) {
//	            }
//		    }
//		}
//
//		// queues messages and writes them to an output stream
//		private static class InputPiper implements java.lang.Runnable {
//			static final String NO_MORE_MESSAGES = UUID.randomUUID().toString();
//			
//			private java.io.OutputStream output;
//			private LinkedBlockingQueue<String> queue;
//			
//			public InputPiper(OutputStream output) {
//				this.output = output;
//				this.queue = new LinkedBlockingQueue<String>();
//			}
//			
//			public void next(String msg) throws Exception {
//				this.queue.add(msg);
//			}
//			
//			public void close() {
//				this.queue.add(NO_MORE_MESSAGES);
//			}
//			
//			public void run() {				
//				try {
//					while (true) {
//						String s = queue.take();
//						if (s.equals(NO_MORE_MESSAGES))
//							break;
//						output.write(s.getBytes("utf-8"));
//						output.flush();
//					}
//				} catch (Exception e) {
//					throw new RuntimeException("Broken pipe", e);
//				} finally {
//					try {
//						output.close();
//					} catch (Exception e) {}
//				}
//			}
//		}
//
//		// reads lines from an input stream and queues them
//		private static class OutputQueuePiper implements java.lang.Runnable {
//			
//			private java.io.InputStream input;
//			private BufferedReader reader;
//			private LinkedBlockingQueue<String> queue;
//			private boolean isFirst = true;
//			
//			public OutputQueuePiper(InputStream input) throws Exception {
//				this.input = input;
//				this.reader = new BufferedReader(new InputStreamReader(input, "utf-8"));
//				this.queue = new LinkedBlockingQueue<String>();
//			}
//			
//			public void run() {
//				try {
//					String l = null;
//					while ((l = reader.readLine()) != null)
//						this.queue.add(l);					
//				} catch (Exception e) {
//					throw new RuntimeException("Broken pipe", e);
//				} finally {
//					try {
//						reader.close();
//					} catch (Exception e) {}
//					try {
//						input.close();
//					} catch (Exception e) {}
//				}
//			}
//			
//			public String next() {
//				try {
//					// for the first sentence, we give more time, since
//					// it takes a while to set things up
//					if (isFirst) {
//						isFirst = false;
//						//return this.queue.poll(60, TimeUnit.SECONDS);
//						return this.queue.poll(60, TimeUnit.SECONDS);
//					} else 
//						return this.queue.poll(30, TimeUnit.SECONDS);
//					
//				} catch (InterruptedException e) {
//					System.out.println("ERROR: TIMEOUT");
//					// TODO: need to reset processes here somehow!
//					return null;
//				}
//			}
//			
//			public void close() {
//				try {
//					reader.close();
//				} catch (Exception e) {}
//				try {
//					input.close();
//				} catch (Exception e) {}
//			}
//		}
//		
//		public static class OutputPrintPiper implements java.lang.Runnable {
//			private java.io.InputStream input;
//			private BufferedReader reader;
//			
//			public OutputPrintPiper(InputStream input) throws Exception {
//				this.input = input;
//				this.reader = new BufferedReader(new InputStreamReader(input, "utf-8"));
//			}
//			
//			public void run() {
//				try {
//					String l = null;
//					while ((l = reader.readLine()) != null)
//						System.out.println(l);
//				} catch (Exception e) {
//					throw new RuntimeException("Broken pipe", e);
//				} finally {
//					try {
//						reader.close();
//					} catch (Exception e) {}
//					try {
//						input.close();
//					} catch (Exception e) {}
//				}
//			}
//			
//			public void close() {
//				try {
//					reader.close();
//				} catch (Exception e) {}
//				try {
//					input.close();
//				} catch (Exception e) {}
//			}
//		}
//}
