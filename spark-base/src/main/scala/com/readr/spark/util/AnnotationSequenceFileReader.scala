package com.readr.spark.util

import scala.io.Source
import scala.collection.mutable._
//import com.readr.service.db.client._
import org.apache.hadoop.io._
import org.apache.spark._
import org.apache.spark.SparkContext._
import java.io.ByteArrayOutputStream
import com.esotericsoftware.kryo._
import com.esotericsoftware.kryo.io._
import org.apache.spark.serializer._
import java.io._
import java.util.ArrayList
import com.twitter.chill.ScalaKryoInstantiator
import java.io._
import java.io.ByteArrayOutputStream
import java.util.Arrays
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.io._
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.SequenceFile
import org.apache.spark.SparkContext._
import com.esotericsoftware.kryo._
import com.esotericsoftware.kryo.io._
import com.twitter.chill.ScalaKryoInstantiator
import java.lang._
import java.util.Comparator
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.PathFilter


// create hadoop sequencefile with kryo serialized source

class AnnotationSequenceFileReader(conf:Configuration, typs:Array[Class[_]], targets:String*) {

  private val sfrs = new Array[SequenceFileValueStream](targets.length)
  private val baos = new ByteArrayOutputStream()
  private val lw = new LongWritable()
  private val bw = new BytesWritable()
  private val instantiator = new ScalaKryoInstantiator
  instantiator.setRegistrationRequired(true)
  val kryo = instantiator.newKryo()

  val fs = FileSystem.get(conf)
  for (i <- 0 until targets.length) {
    val path = new Path(targets(i))
	if (!fs.isDirectory(path)) 
	  sfrs(i) = new SequenceFileValueStream(conf, Array(path))
	else {
	  val fstats = fs.listStatus(path, new PathFilter() {
		override def accept(f:Path):scala.Boolean = {
		  f.getName().startsWith("part-")
		}
	  });
	  Arrays.sort(fstats, new Comparator[FileStatus]() {
		override def compare(f1:FileStatus, f2:FileStatus):Int = {
		  f1.getPath().getName().compareTo(f2.getPath().getName())
		}
	  });
	  val fsis = new Array[Path](fstats.length)
	  for (j <- 0 until fstats.length) fsis(j) = fstats(j).getPath()
	  this.sfrs(i) = new SequenceFileValueStream(conf, fsis)
	}
  }

  def register(clazz:Class[_]) = {
    kryo.register(clazz)
  }

  def read():Array[Any] = {
    var arr = new Array[Any](this.sfrs.length)
    for (i <- 0 until this.sfrs.length) {
      val ok = this.sfrs(i).next
      if (!ok) return null
      val v = this.sfrs(i).getValue
      val input = new Input(v.getBytes())
      arr(i) = kryo.readObject(input, typs(i))
    }
    return arr
  }
	
  def close = {
	for (i <- 0 until sfrs.length) sfrs(i).close
  }
	
  class SequenceFileValueStream(conf:Configuration, sequenceFiles:Array[Path]) {
	val lw = new LongWritable()
	val bw = new BytesWritable();
	var current:Int = 0
		
	val readers = new Array[SequenceFile.Reader](sequenceFiles.length)
	for (i <- 0 until readers.length)
	  readers(i) = new SequenceFile.Reader(FileSystem.get(conf), sequenceFiles(i), conf)
		
	def getKey = lw
		
	def getValue = bw
		
	def next:Boolean = {
	  val succ = readers(current).next(lw, bw)
	  if (succ) return succ
	  if (current < readers.length-1) {
	    current += 1
		return next
	  }
	  return false
	}
		
	def close = {
	  for (i <- 0 until readers.length)
	    readers(i).close
	}
  }
}