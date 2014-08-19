package com.readr.spark.util

import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.IOException
import java.nio.channels.FileChannel

object FileOperations {
 
  def move(from:String, to:String):Unit = {
	val fromFile = new File(from)
	val toFile = new File(to)
		
	// if target exists delete
	if (toFile.exists) toFile.delete
		
	// try if renaming works (e.g. if on same drive, platform-dependent)
	fromFile.renameTo(toFile)
	if (toFile.exists) return
		
	// must copy file
    val inChannel = new FileInputStream(from).getChannel
    val outChannel = new FileOutputStream(to).getChannel
	try {
      // magic number for Windows, 64Mb - 32Kb)
      val maxCount = (64 * 1024 * 1024) - (32 * 1024)
      var size:Long = inChannel.size
      var position:Long = 0
      while (position < size) {
        position += 
          inChannel.transferTo(position, maxCount, outChannel);
      }
	} catch { 
	  case e:IOException => throw e
	} finally {
	  if (inChannel != null) inChannel.close
	  if (outChannel != null) outChannel.close
	}
	fromFile.delete
  }
  
  def copy(from:String, to:String) = {
	val toFile = new File(to)
		
	// if target exists delete
	if (toFile.exists) toFile.delete
		
	// must copy file
    val inChannel = new FileInputStream(from).getChannel
    val outChannel = new FileOutputStream(to).getChannel
	try {
      // magic number for Windows, 64Mb - 32Kb)
      val maxCount = (64 * 1024 * 1024) - (32 * 1024);
      val size:Long = inChannel.size
      var position:Long = 0
      while (position < size)
        position += inChannel.transferTo(position, maxCount, outChannel)
	} catch {
	  case e:IOException => throw e;
	} finally {
	  if (inChannel != null) inChannel.close
	  if (outChannel != null) outChannel.close
	}
  }

  def rmr(dir:String):Unit = {
	val f = new File(dir)
	if (f.exists) {			
	  if (!f.isDirectory) {
		f.delete
	  } else {
		for (fi <- f.listFiles)
		  rmr(fi.getAbsolutePath)
	    f.delete
	  }
	}
  }
	
  def remove(file:String) = {
	val f = new File(file);
	if (f.exists) f.delete
  } 
}