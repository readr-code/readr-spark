package com.readr.spark

import java.util.Comparator
import java.util.Arrays
import java.util.regex.Pattern
import java.io.File
import java.io.ByteArrayOutputStream
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.join.CompositeInputFormat
import org.apache.hadoop.mapred.join.TupleWritable
import org.apache.hadoop.mapred.InputFormat
import org.apache.hadoop.mapred.SequenceFileInputFormat
import org.apache.hadoop.mapred.SequenceFileOutputFormat
import org.apache.hadoop.mapred.FileOutputFormat
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.BytesWritable
import org.apache.spark.rdd.RDD
import com.esotericsoftware.kryo.io.Input
import com.twitter.chill._
import org.apache.hadoop.fs.Path
import com.readr.spark.util.Annotator
import scala.Predef._
import com.readr.model.annotation.Annotations
import org.apache.spark.serializer.KryoRegistrator
import scala.collection.mutable.ArrayBuffer
import com.readr.spark.util.Utils._
import scala.collection.mutable.Map
import scala.collection.mutable.Buffer
//import scala.util.DynamicVariable

class MyRegistrator extends KryoRegistrator {
// note: order of registrations must be identical for both
// reading and writing a kryo file
  override def registerClasses(kryo:Kryo) {
    for (clazz <- Annotations.annWithDependentClazzes)
      kryo.register(clazz)    
  }
}

case class Schema(
  annTyps:Buffer[Class[_]] = Buffer(),
  defaults:Map[Class[_],Int] = Map(),
  provenance:Buffer[String] = Buffer()
)


object rr {
  val usage = """
    Usage: run
  """

    
  val tl = new ThreadLocal[KryoBase] {
    override def initialValue:KryoBase = {
      val instantiator = new ScalaKryoInstantiator
      instantiator.setRegistrationRequired(true)
      val kryo = instantiator.newKryo()
    
      val classLoader = Thread.currentThread.getContextClassLoader
      kryo.setClassLoader(classLoader)

      for (clazz <- Annotations.annWithDependentClazzes)
        kryo.register(clazz)    
      kryo
    }
  }

  /** Read documents with annotations.
      Annotation types are detected based on file name endings. */
  def read(dir:String, names:Array[String])(implicit sc:SparkContext):RDD[(Long,Array[Any])] = {
    //sc.getConf.set("spark.kryo.registrator", "com.readr.spark.rr.MyRegistrator")
    
    val annTyps = names.map(annTyp)
    val inputPaths:Array[String] = names.map(dir + "/" + _)
    val conf = localHadoopConf
	val jobConf = new JobConf(conf, classOf[App])
	jobConf.setInputFormat(classOf[CompositeInputFormat[LongWritable]])
	jobConf.set("mapred.join.expr", CompositeInputFormat.compose("inner", 
			classOf[SequenceFileInputFormat[LongWritable,BytesWritable]], inputPaths: _*))
	val inputFormatClass:Class[_ <: InputFormat[LongWritable,TupleWritable]] = 
	  classOf[CompositeInputFormat[LongWritable]]
	val keyClass = classOf[LongWritable]
	val valueClass = classOf[TupleWritable]
	val minSplits = 1
	val rdd = sc.hadoopRDD[LongWritable, TupleWritable](jobConf, inputFormatClass, 
	    keyClass, valueClass, minSplits)

    rdd.map(unpack(annTyps, _)) //, tl))
  }

  /** Read documents with annotations.
      Discover column files and detect annotation types based on file endings. */
  def read(dir:String)(implicit sc:SparkContext):RDD[(Long,Array[Any])] = {
    val names = findFileNames(dir).map(x => x._2)
    read(dir, names)
  }

  def read(dir:String, schema:Schema)(implicit sc:SparkContext):RDD[(Long,Array[Any])] = {
    val names = findFileNames(dir).map(x => x._2)
    schema.annTyps ++= names.map(annTyp)
    for (i <- 0 until schema.annTyps.size) schema.defaults.put(schema.annTyps(i), i)
    read(dir, names)
  }

  /** Write documents with annotations. */
  def write(rdd:RDD[(Long,Array[Any])], dir:String, names:Array[String])(implicit sc:SparkContext) = {
    val conf = localHadoopConf
    //val tl = new ThreadLocal[KryoBase]
    
    // save every column separately
    for (i <- 0 until names.length) {
      println("writing " + names(i))
      val outputPath = dir + "/" + names(i)
	  val jobConf = new JobConf(conf, classOf[App])
	  jobConf.setOutputFormat(classOf[SequenceFileOutputFormat[LongWritable,BytesWritable]])
	  jobConf.setOutputKeyClass(classOf[LongWritable])
	  jobConf.setOutputValueClass(classOf[BytesWritable])
	  FileOutputFormat.setOutputPath(jobConf, new Path(outputPath))
      
      val crdd = rdd.map(v => {
        val ann = v._2(i)
        val s = serialize(ann) //, tl)
        (new LongWritable(v._1), 
          new BytesWritable(s))
      })
      crdd.saveAsHadoopDataset(jobConf)
    }
  }
  
  /** Write documents with annotations. 
   *  Create default file names of the form data.col{NUM}.{TYPE}, eg.
   *  data.col12.tokenoffsets */  
  def write(rdd:RDD[(Long,Array[Any])], dir:String)(implicit sc:SparkContext):Unit = {
    if (rdd.count == 0) return
    
    // use first row to determine output names and types
    val names = new Array[String](rdd.first._2.length)
    val fa = rdd.first._2.map(x => x)
    
    for (i <- 0 until fa.length)
      names(i) = s"data.col${i}.${fa(i).getClass.getSimpleName}"
    write(rdd, dir, names)
  }

  /** Write documents with annotations. 
   *  Create default file names of the form data.col{NUM}.{TYPE}, eg.
   *  data.col12.tokenoffsets */  
  def write(rdd:RDD[(Long,Array[Any])], dir:String, schema:Schema)(implicit sc:SparkContext):Unit = {
    if (rdd.count == 0) return
    
    // use first row to determine output names and types
    val names = new Array[String](schema.annTyps.length)
    
    for (i <- 0 until names.length)
      names(i) = s"data.col${i}.${schema.annTyps(i).getSimpleName}"
    write(rdd, dir, names)
  }

  /** Run annotator, append results to annotations array */
  def annotate2(rdd:RDD[(Long,Array[Any])], annotator:Annotator, 
      args:Int*)(implicit sc:SparkContext):RDD[(Long,Array[Any])] = {
    // verify that we have right args for annotator
    require(args.length == annotator.requires.length, 
        s"""${annotator.getClass().getSimpleName()} requires ${annotator.requires.length} args, but provided ${args.length} """)
    rdd.map(x => {
      val anns = x._2
      val out = annotator.annotate(args.map(anns(_)):_*)
      (x._1, x._2 ++ out)
    })  
  }  

  /** Run annotator, append results to annotations array */
  def annotate(rdd:RDD[(Long,Array[Any])], annotator:Annotator)(implicit sc:SparkContext):RDD[(Long,Array[Any])] = {
    val arr = ArrayBuffer[Int]()
    for (i <- 0 until annotator.requires.length)
      arr += firstColumnOfType(rdd, annotator.requires(i))
    val args = arr.toArray
    rdd.map(x => {
      val anns = x._2
      val out = annotator.annotate(args.map(anns(_)):_*)
      (x._1, x._2 ++ out)
    })  
  }  

  /** Run annotator, append results to annotations array */
  def annotate(rdd:RDD[(Long,Array[Any])], annotator:Annotator, schema:Schema, 
      bindings:scala.collection.immutable.Map[Int,Int] = scala.collection.immutable.Map())(implicit sc:SparkContext):RDD[(Long,Array[Any])] = {
    val arr = ArrayBuffer[Int]()
    for (i <- 0 until annotator.requires.length)
      arr += schema.defaults(annotator.requires(i))
    for ((k,v) <- bindings)
      arr(k) = v
      
    for (i <- 0 until annotator.generates.length) {
      schema.annTyps += annotator.generates(i)
      schema.provenance += annotator.toString      
    }
    
    // set defaults
    for (i <- 0 until schema.annTyps.size) schema.defaults.put(schema.annTyps(i), i)
    val args = arr.toArray
    rdd.map(x => {
      //println("document " + x._1)
      val anns = x._2
      val out = annotator.annotate(args.map(anns(_)):_*)
      (x._1, x._2 ++ out)
    })  
  }  

  private def localHadoopConf:Configuration = {
    val conf = new Configuration();
	conf.set("mapred.job.tracker", "local")
	conf.set("dfs.replication", "1")
	conf.set("fs.default.name", "file:///")
	conf.set("job.local.dir", "/tmp")
    conf
  }
  
  private def serialize(o:Any/*, tl:ThreadLocal[KryoBase]*/):Array[Byte] = {
    val kryo = tl.get //getSetThreadLocalKryo(tl)
    val baos = new ByteArrayOutputStream()
    val kryout = new Output(baos, 4096)
    kryo.writeObject(kryout, o)
    kryout.flush
	baos.close
    baos.toByteArray
  }
  
  private def annTyp(file:String):Class[_] = {
    val pos = file.lastIndexOf(".")
    val ending = file.substring(pos + 1)
//    ending match {
//      case "ConstituentAnn" => classOf[ConstituentAnn]
//      case "CorefAnn" => classOf[CorefAnn]
//      case "DependencyAnn" => classOf[DependencyAnn]
//      case "FrameMatchAnn" => classOf[FrameMatchAnn]
//      case "FrameMatchFeatureAnn" => classOf[FrameMatchFeatureAnn]
//      case "LemmaAnn" => classOf[LemmaAnn]
//      case "NERAnn" => classOf[NERAnn]
//      case "ParseAnn" => classOf[ParseAnn]
//      case "POSAnn" => classOf[POSAnn]
//      case "SentenceOffsetAnn" => classOf[SentenceOffsetAnn]
//      case "SentenceTokenOffsetAnn" => classOf[SentenceTokenOffsetAnn]
//      case "Source" => classOf[Source]
//      case "TextAnn" => classOf[TextAnn]
//      case "TextFragmentAnn" => classOf[TextFragmentAnn]
//      case "TextMappingAnn" => classOf[TextMappingAnn]
//      case "TokenOffsetAnn" => classOf[TokenOffsetAnn]
//      case "TokensAnn" => classOf[TokensAnn]
//    }
    
    for (clazz <- Annotations.annClazzes)
      if (clazz.getSimpleName.equals(ending)) 
        return clazz
    assert(false, "Unknown extension: " + ending)
    null
  }    
    
  private def unpack(annTyps:Array[Class[_]], x:Tuple2[LongWritable,TupleWritable]): //, tl:ThreadLocal[KryoBase]):
     Tuple2[Long,Array[Any]] = {
    val kryo = tl.get
    
    val r = new Array[Any](annTyps.length)
    for (i <- 0 until annTyps.length) {
      val bw = x._2.get(i).asInstanceOf[BytesWritable]
      val input = new Input(bw.getBytes)
      val o = kryo.readObject(input, annTyps(i))
      r(i) = o
    }
    (x._1.get(), r)
  }
  
  val filePattern = Pattern.compile("data\\.col(\\d+)\\.(.+)")
  
  private def findFileNames(dir:String):Array[(Int,String)] = {
    val fs = new File(dir)
    val l = ArrayBuffer[(Int,String)]()
    for (f <- fs.list) {
      val m = filePattern.matcher(f)
      if (m.find) {
        val col = m.group(1).toInt
        val typ = m.group(2)
        l += Tuple2(col, f)
      }
    }
    val arr = l.toArray
    Arrays.sort(arr, new Comparator[(Int,String)]() {
      def compare(a:(Int,String), b:(Int,String)) =
        a._1 - b._1
    })
    arr
  }

}
