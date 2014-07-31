package com.readr.spark.util

import scala.io.Source
import scala.collection.mutable._
import org.apache.hadoop.io._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.join.CompositeInputFormat;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import java.io.ByteArrayOutputStream

import com.esotericsoftware.kryo._
import com.esotericsoftware.kryo.io._
import com.twitter.chill.ScalaKryoInstantiator

import java.util.Properties

  class MyMapper extends MapReduceBase with
	Mapper[LongWritable, TupleWritable, LongWritable, BytesWritable] {
	
    var inputTypes:Array[Class[_]] = null
    var outputType:Class[_] = null
    
    //def inputTypes_=(x$1: Array[Class[_]]): Unit = ??? 
    //def outputType_=(x$1: Class[_]): Unit = ???
		
	private var annotator:Annotator = null

	private val instantiator = new ScalaKryoInstantiator
    instantiator.setRegistrationRequired(true)
    val kryo = instantiator.newKryo()

	override def configure(jobConf:JobConf) = {
	  val localDir = jobConf.get("job.local.dir")
	  val properties = new Properties()
	  properties.setProperty("tmpDir", localDir)
			
	  try {
		val clz = Annotate.classByName(jobConf.get(Annotate.ANNOTATOR_KEY)).asSubclass(classOf[Annotator])

		annotator = clz.getDeclaredConstructor(). //classOf[Properties]).
			newInstance() //(properties)
				
		inputTypes = annotator.requires
		outputType = annotator.generates(0) //hack
				
	   } catch {
	     case e:Exception =>
		   e.printStackTrace()
	   }			
	   if (annotator != null)
	     annotator.init
	}
		
	override def close = {
	  if (annotator != null)
		annotator.close
	}
		
	override def map(key:LongWritable, v:TupleWritable, 
		output:OutputCollector[LongWritable, BytesWritable], reporter:Reporter) {
			
	  val ins = new Array[Any](inputTypes.length)
	  for (i <- 0 until inputTypes.length) {
	    
		val bw = v.get(i).asInstanceOf[BytesWritable]
		val input = new Input(bw.getBytes())
        ins(i) = kryo.readObject(input, inputTypes(i))
	  }
	  
	  val out = annotator.annotate(ins:_*)

	  val baos = new ByteArrayOutputStream()
      val kryout = new Output(baos, 4096)
      kryo.writeObject(kryout, out)
      kryout.flush
      val bytes = baos.toByteArray
	  baos.close
			
	  val lw = new LongWritable()
	  lw.set(key.get())
	  val bw = new BytesWritable()
	  bw.set(bytes, 0, bytes.length)
	  output.collect(lw, bw)
	}
  }

object Annotate {

  val ANNOTATOR_KEY = "readr.annotator"
  
  def classByName(name:String):Class[_ <: Annotator] = {
	Class.forName(name).asInstanceOf[Class[Annotator]]//asSubclass(Class[AbstractAnnotator])
  }

}

class Annotate {
  

  def execute(conf:Configuration, argDir:String, numPartitions:Int, argAnnotator:String, 
			outIn:Array[String], overwrite:Boolean):Boolean = { 
	val clz = Annotate.classByName(argAnnotator)
	execute(conf, argDir, numPartitions, clz, outIn, overwrite)
  }
	
  def execute[T <: Annotator](conf:Configuration, argDir:String, numPartitions:Int, 
		clz:Class[T], outIn:Array[String], overwrite:Boolean):Boolean = {
	val argAnnotator = clz.getName()
	// get input and output file names
	
	val annotator = clz.getDeclaredConstructor(). //classOf[Properties]).
		newInstance() //(new Properties())
		
	val defaultOutIn = IOPathUtils.getDefaultOutIn(annotator)
	val inputPaths = IOPathUtils.getInputPaths(argDir, outIn, defaultOutIn)
	val outputPath = IOPathUtils.getOutputPath(argDir, outIn, defaultOutIn)
	IOPathUtils.logInputOutputPaths(outputPath, inputPaths)

	if (overwrite) {
	  val fs = FileSystem.get(conf)
	  if (fs.exists(outputPath)) fs.delete(outputPath, true)
	}
		
	val jobConf = new JobConf(conf, classOf[Annotate])
	jobConf.set(Annotate.ANNOTATOR_KEY, argAnnotator);
		
	jobConf.setInputFormat(classOf[CompositeInputFormat[_]])
	jobConf.set("mapred.join.expr", CompositeInputFormat.compose("inner", 
			classOf[SequenceFileInputFormat[LongWritable,BytesWritable]], inputPaths: _*))

	jobConf.setMapperClass(classOf[MyMapper])
	jobConf.setReducerClass(classOf[IdentityReducer[_,_]])
	jobConf.setNumReduceTasks(numPartitions)
		
	jobConf.setOutputFormat(classOf[SequenceFileOutputFormat[_,_]])
	jobConf.setOutputKeyClass(classOf[LongWritable])
	jobConf.setOutputValueClass(classOf[BytesWritable])
		
	FileOutputFormat.setOutputPath(jobConf, outputPath)
		
	val rj = JobClient.runJob(jobConf)
	rj.isSuccessful()
  }
}