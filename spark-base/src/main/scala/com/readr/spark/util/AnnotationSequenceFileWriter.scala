package com.readr.spark.util

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

class AnnotationSequenceFileWriter(conf:Configuration, path:String) {
  
  private val instantiator = new ScalaKryoInstantiator
  instantiator.setRegistrationRequired(true)
  val kryo = instantiator.newKryo()

  private val baos = new ByteArrayOutputStream()
  private val lw = new LongWritable()
  private val bw = new BytesWritable()
    
  val fs = FileSystem.get(conf)
  val targetPath = new Path(path)
  FileSystem.mkdirs(fs, targetPath, FsPermission.getDefault())
  val outPath = new Path(targetPath, "part-00000")
		// boolean overwrite = true;
		// this.dos = fs.create(path, overwrite);
  val writer = SequenceFile.createWriter(fs, conf, outPath, 
	classOf[LongWritable], classOf[BytesWritable], 
	SequenceFile.CompressionType.NONE)
  
  def register(clazz:Class[_]) = {
    kryo.register(clazz)
  }

  def write(a:Any):Unit = {
	write(-1, a)
  }
  
  def write(id:Int, a:Any):Unit = {
	// key
	lw.set(id)
	bw.set(Array[scala.Byte](), 0, 0)
	// value
	this.baos.reset()
    val baos = new ByteArrayOutputStream()
    val output = new Output(baos, 4096)
    kryo.writeObject(output, a)
    val bytes = output.toBytes
    println(bytes.length + " bytes")
	bw.set(bytes, 0, bytes.length)
	this.writer.append(lw, bw)
  }

  def close = {
	this.writer.close
  }  
}