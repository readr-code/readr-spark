package com.readr.spark.cj

import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader
import java.nio.file.FileSystems
import java.nio.file.attribute.PosixFilePermissions
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.StringBuilder
import scala.util.control.Breaks.break
import com.readr.spark.util.Annotator
import com.readr.model.annotation.TextAnn
import com.readr.model.annotation.TokensAnn
import com.readr.model.annotation.ParseAnn
import com.readr.model.annotation.SentenceTokenOffsetAnn

// note: to compile the cj parser on some platforms, one needs to add #include <getopt.h>
// to second-stage/features/best-parses.cc

// If you compile this on Mac, make sure you use the gcc compiler, not clang which is default
// with macports you can switch
//   sudo port install gcc_select
//   sudo port select --list gcc
//   sudo port select gcc llvm-gcc42
//   readlink `which c++`


// test, using:
//cat steedman.txt | first-stage/PARSE/parseIt -l399 -N50 -K first-stage/DATA/EN/ | second-stage/programs/features/best-parses -l second-stage/models/ec50spfinal/features.gz second-stage/models/ec50spfinal/cvlm-l1c10P1-weights.gz

class CJParser extends Annotator(
      generates = Array(classOf[ParseAnn]),
      requires = Array(classOf[TokensAnn], classOf[SentenceTokenOffsetAnn])) {
  
  //val dir = "/tmp/cj/reranking-parser"
  val cmd1 = "first-stage/PARSE/parseIt"
  val cmd2 = "second-stage/programs/features/best-parses"
  val modelDir = "second-stage/models/ec50spfinal"
  val estimatorNickname = "cvlm-l1c10P1"
			
  // if tmp cj dir does not exist, then unpack cj from resource zip first
  //val tmpDir = properties.getProperty("tmpDir", "/tmp");
  //readrTmpDir = tmpDir + "/readr-data_" + System.currentTimeMillis();
  //readrTmpDir = tmpDir;
  var readrTmpDir = "/tmp";
  println("tmpDir: " + readrTmpDir)
	
  //def init = {
	val tmpDirFile = new File(readrTmpDir)
	//if (tmpDirFile.exists()) tmpDirFile.delete();

	val dir = readrTmpDir + "/cj/reranking-parser"

	if (!new File(dir).exists) {
	  tmpDirFile.mkdirs
			
	  //URI jarURI2 = CJParseAnnotator.class.getProtectionDomain().getCodeSource().getLocation().toURI();
	  System.out.println("extracting cj.zip")
	  val is = classOf[CJParser].getResourceAsStream("cj.zip")
	  com.readr.spark.util.FileUtils.dumpStreamToFile(is, "cj.zip", readrTmpDir)
			
	  //URI jarURI = url.toURI();
	  //System.out.println(jarURI.toString());
	  //URI jarURI = com.readr.process.util.FileUtils.getJarFromClass(CJParseAnnotator.class);
	  //System.out.println("now extracting");
	  //com.readr.process.util.FileUtils.extractFromZip(jarURI, "cj.zip", readrTmpDir);
	  com.readr.spark.util.FileUtils.extractZipFile(readrTmpDir + "/" + "cj.zip", readrTmpDir);
	  for (s <- tmpDirFile.list)
		System.out.println("   file: " + s);
			
	  // chmod
	  // java 6
	  //new File(readrTmpDir + "/cj/reranking-parser" + "/" + cmd1).setExecutable(true);
	  //new File(readrTmpDir + "/cj/reranking-parser" + "/" + cmd2).setExecutable(true);

	  // the following is Java 7 only
      java.nio.file.Files.setPosixFilePermissions(
          FileSystems.getDefault().getPath(readrTmpDir + "/cj/reranking-parser", cmd1),
		  PosixFilePermissions.fromString("rwxr-x---"))
	  java.nio.file.Files.setPosixFilePermissions(
		  FileSystems.getDefault().getPath(readrTmpDir + "/cj/reranking-parser", cmd2),
		  PosixFilePermissions.fromString("rwxr-x---"))
    }

  //@transient var pb1:ProcessBuilder = null
  //@transient var pb2:ProcessBuilder = null
  //@transient var p1:Process = null
  //@transient var p2:Process = null
  //@transient var pipe:Piper = null
//  @transient var inputQueuePipe:InputPiper = null
//  @transient var outputQueuePipe:OutputQueuePiper = null
//  @transient var error1Pipe:OutputPrintPiper = null
//  @transient var error2Pipe:OutputPrintPiper = null
  @transient lazy val myproc = new ThreadLocal[All]
  
  @transient class All {
   var pb1 = new ProcessBuilder(cmd1, "-l399", "-N50", "-K", "first-stage/DATA/EN/")
   var pb2 = new ProcessBuilder(cmd2, "-l", modelDir + "/features.gz", 
	   modelDir + "/" + estimatorNickname + "-weights.gz");

	pb1.directory(new File(dir))
	pb2.directory(new File(dir))
		
   var p1 = pb1.start
   var p2 = pb2.start
		
   var pipe = new Piper(p1.getInputStream(), p2.getOutputStream)
   var inputQueuePipe = new InputPiper(p1.getOutputStream)
   var outputQueuePipe = new OutputQueuePiper(p2.getInputStream)
   var error1Pipe = new OutputPrintPiper(p1.getErrorStream)
   var error2Pipe = new OutputPrintPiper(p2.getErrorStream)
		
	new Thread(pipe).start
	new Thread(outputQueuePipe).start
	new Thread(inputQueuePipe).start
	new Thread(error1Pipe).start
	new Thread(error2Pipe).start

	  def reset = {
	// sometimes I get the following error at a job begin here:
	// java.io.IOException: Cannot run program "first-stage/PARSE/parseIt" (in directory "/mnt2/var/lib/hadoop/mapred/taskTracker/hadoop/jobcache/job_201302142005_0001/work/readr-data_1360872723154/cj/reranking-parser"): java.io.IOException: error=2, No such file or directory
				
	p1 = pb1.start
	p2 = pb2.start
		
	pipe = new Piper(p1.getInputStream(), p2.getOutputStream)
	inputQueuePipe = new InputPiper(p1.getOutputStream)
	outputQueuePipe = new OutputQueuePiper(p2.getInputStream)
	error1Pipe = new OutputPrintPiper(p1.getErrorStream)
	error2Pipe = new OutputPrintPiper(p2.getErrorStream)
		
	new Thread(pipe).start
	new Thread(outputQueuePipe).start
	new Thread(inputQueuePipe).start
	new Thread(error1Pipe).start
	new Thread(error2Pipe).start
  }

	   def close = {
    println("CLOSE")
     // destroy external processes
	if (p1 != null)
	  try { p1.destroy } catch { case e:Exception => }
	if (p2 != null)
	  try { p2.destroy } catch { case e:Exception => }

	// close all piping threads
	if (pipe != null)
	  pipe.close
	if (inputQueuePipe != null)
	  inputQueuePipe.close
	if (outputQueuePipe != null)
	  outputQueuePipe.close
	if (error1Pipe != null)
	  error1Pipe.close
	if (error2Pipe != null)
	  error2Pipe.close
  }

  }
  
	
	
	
	//reset
  //}
	
	
  override def annotate(ins:Any*):Array[Any] = {
    Array(run(ins(0).asInstanceOf[TokensAnn], 
        ins(1).asInstanceOf[SentenceTokenOffsetAnn]))
  }

  def run(toa:TokensAnn, sto:SentenceTokenOffsetAnn):ParseAnn = {
    var all = myproc.get()
    if (all == null) {
      all = new All()
      myproc.set(all)
    }
    
    
    
    val ppsens = new ArrayBuffer[String]()
    
    for (sen <- sto.sents) {
	  // this step is necessary to report progress to hadoop,
	  // so that it doesn't kill this task if there is a document
	  // with very many sentences
	  if (reporter != null) reporter.incrementCounter();
	
	  val toks = toa.tokens.slice(sen.f, sen.t)
				
	  val sb = new StringBuilder
	  sb.append("<s>")
	  for (tok <- toks)
		sb.append(" " + tok)
	  sb.append(" </s>\n")
	  val inputSentence = sb.toString
	  //String inputSentence = "<s> This cat likes fresh fish. </s>\n";

	  // send to parser
	  try {
		all.inputQueuePipe.next(inputSentence)
	  } catch {
	    case e1:Exception => e1.printStackTrace
	  }
			
	  // wait for output
	  val tree = all.outputQueuePipe.next
	  if (tree == null) {				
		try {
		  println("RESET")
		  close
		  all.reset
		} catch {
		  case e:Exception => e.printStackTrace
		}
		//tree = "";
	  }
	  //trees.add(tree);
			
	  ppsens += tree
			
	  //println("Tree for: " + inputSentence)
	  //println(tree)
	  //println(sop.getFile() + "\t" + sentNum);
	}
	ParseAnn(ppsens.toArray)
  }
	
  override def close = {
    var all = myproc.get()
    if (all != null) {
      all.close
    }
  }

	
  // connects an input stream to an output stream 
  class Piper(input:java.io.InputStream, output:java.io.OutputStream) extends java.lang.Runnable {

	override def run = {
	  try {
		val b = new Array[Byte](512)
		var read = 1
		while ({read = input.read(b, 0, b.length); read > -1}) {
		  output.write(b, 0, read)
		  output.flush
		}
	  } catch {
	    case e:Exception => throw new RuntimeException("Broken pipe", e)
	  } finally {
	    close
	  }
	}
	
    def close = {
	  try {
		output.close
	  } catch { case e:Exception => }
	  try {
		input.close
	  } catch { case e:Exception => }
	}
  }


  // queues messages and writes them to an output stream
  class InputPiper(output:java.io.OutputStream) extends java.lang.Runnable {
	val NO_MORE_MESSAGES = UUID.randomUUID().toString
			
	val queue = new LinkedBlockingQueue[String]()
			
	def next(msg:String) = {
	  this.queue.add(msg)
	}
			
	def close = {
	  this.queue.add(NO_MORE_MESSAGES)
	}
			
	override def run = {				
	  try {
		while (true) {
		  val s = queue.take
		  if (s.equals(NO_MORE_MESSAGES))
			  break
		  output.write(s.getBytes("utf-8"))
		  output.flush()
		}
	  } catch {
	    case e:Exception => throw new RuntimeException("Broken pipe", e)
	  } finally {
		try {
		  output.close
		} catch { case e:Exception => }
	  }
	}
  }

  // reads lines from an input stream and queues them
  class OutputQueuePiper(input:java.io.InputStream) extends java.lang.Runnable {
	val reader = new BufferedReader(new InputStreamReader(input, "utf-8"))
	val queue = new LinkedBlockingQueue[String]()			
	var isFirst = true
			
    override def run = {
	  try {
		var l:String = null
		while ({l = reader.readLine; l != null})
		  this.queue.add(l)			
	  } catch {
	    case e:Exception => throw new RuntimeException("Broken pipe", e)
	  } finally {
		close
	  }
	}
			
	def next:String = {
	  try {
		// for the first sentence, we give more time, since
		// it takes a while to set things up
		if (isFirst) {
		  isFirst = false
		  //return this.queue.poll(60, TimeUnit.SECONDS)
		  return this.queue.poll(60, TimeUnit.SECONDS)
		} else 
		  return this.queue.poll(30, TimeUnit.SECONDS)
					
	  } catch {
	    case e:InterruptedException => {
		  System.out.println("ERROR: TIMEOUT")
		  // TODO: need to reset processes here somehow!
		  return null
	    }
	  }
	}
			
	def close = {
	  try {
		reader.close
	  } catch { case e:Exception => }
	  try {
		input.close
	  } catch { case e:Exception => }
	}
  }
		
  class OutputPrintPiper(input:java.io.InputStream) extends java.lang.Runnable {
    private val reader = new BufferedReader(new InputStreamReader(input, "utf-8"))
			
	override def run = {
	  try {
		var l:String = null
		while ({l = reader.readLine; l != null})
		  println(l)
	  } catch {
	    case e:Exception => throw new RuntimeException("Broken pipe", e)
	  } finally {
	    close
	  }
	}
			
	def close = {
	  try {
		reader.close
	  } catch { case e:Exception => }
	  try {
		input.close
	  } catch { case e:Exception => }
	}
  }		
}
