package com.readr.spark.util

import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.File
import java.io.FileNotFoundException
import java.io.FileOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.net.URI
import java.net.URISyntaxException
import java.net.URL
import java.security.CodeSource
import java.security.ProtectionDomain
import java.util.Enumeration
import java.util.zip.ZipEntry
import java.util.zip.ZipException
import java.util.zip.ZipFile

object FileUtils {
 
  def dumpStreamToFile(is:InputStream, newFile:String, path:String) = {
	val buf = new Array[Byte](4096)
	var	l = -1
	val o = new BufferedOutputStream(new FileOutputStream(path + "/" + newFile))
	while ({l = is.read(buf); l != -1})
	  o.write(buf, 0, l)
	o.close
	is.close
  }	
	
  def extractZipFile(zipFile:String):Unit = {
	val newPath = zipFile.substring(0, zipFile.length - 4)
	new File(newPath).mkdir
	extractZipFile(zipFile, newPath)
  }

  def extractZipFile(zipFile:String, newPath:String):Unit = {
	val BUFFER = 2048
	val file = new File(zipFile)

	val zip = new ZipFile(file)
	val zipFileEntries = zip.entries

	// Process each entry
	while (zipFileEntries.hasMoreElements) {
	  // grab a zip file entry
	  val entry = zipFileEntries.nextElement
	  val currentEntry = entry.getName
	  val destFile = new File(newPath, currentEntry)
	  //destFile = new File(newPath, destFile.getName());
	  val destinationParent = destFile.getParentFile

	  // create the parent directory structure if needed
	  destinationParent.mkdirs

	  if (!entry.isDirectory) {
	    val is = new BufferedInputStream(zip.getInputStream(entry))
	    // establish buffer for writing file
	    val data = new Array[Byte](BUFFER)

	    // write the current file to disk
	    val fos = new FileOutputStream(destFile)
	    val dest = new BufferedOutputStream(fos, BUFFER)

	    var currentByte:Int = -1
	    // read and write until last byte is encountered
	    while ({currentByte = is.read(data, 0, BUFFER); currentByte != -1})
	      dest.write(data, 0, currentByte)
	    dest.flush
	    dest.close
	    is.close
	  }

//	        if (currentEntry.endsWith(".zip"))
//	        {
//	            // found a zip file, try to open
//	            extractZipFile(destFile.getAbsolutePath(), new File(newPath, currentEntry));
//	        }
	}
	zip.close
  }
	
  ////////////////////////////////////////// jar stuff
	
  def getJarFromClass(javaClass:Class[_]):URI = {
	val domain = javaClass.getProtectionDomain
	val source = domain.getCodeSource
	val url    = source.getLocation
	url.toURI
  }

  def extractFromZip(zipFileUri:URI, resourceName:String, targetDir:String) = {
    val location = new File(zipFileUri)
    	
    if (location.isDirectory) {
      FileOperations.copy(new File(location, resourceName).getAbsolutePath, 
    	new File(targetDir, resourceName).getAbsolutePath)
    } else {
	  var zipFile:ZipFile = null
	        
	  try {
	    zipFile = new ZipFile(location)
	        
		val zipEntry = zipFile.getEntry(resourceName)
		if (zipEntry == null)
		  throw new FileNotFoundException("cannot find file: " + resourceName + " in archive: " + zipFile.getName)
		val simpleName = zipEntry.getName().substring(zipEntry.getName().lastIndexOf("/") + 1); 
		val targetFile = targetDir + "/" + simpleName;
		        
		val zipStream = zipFile.getInputStream(zipEntry)
		val fileStream = new BufferedOutputStream(new FileOutputStream(targetFile));
		
		val b = new Array[Byte](4096)
		var l = 0
		while ({l = zipStream.read(b); l != -1})
		  fileStream.write(b, 0, l);
		        
		zipStream.close
		fileStream.close
	  } catch {
	    case e:Exception => throw e
	  } finally {
	    zipFile.close
	  }
    }
  } 
}