package com.readr.spark.index

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.util.control.Breaks._
import scala.collection.mutable._
import scala.collection.JavaConversions._
import com.readr.model._
import com.readr.model.annotation._
import com.readr.spark.util.Utils._
//import org.apache.hadoop.hdfs.server.namenode.FSDirectory
import java.io.StringReader

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.document.LongField
import org.apache.lucene.document.StringField
import org.apache.lucene.document.TextField
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.index.Term
import org.apache.lucene.store.Directory
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.util.Version
import java.io.BufferedReader
import java.io.File
import java.io.FileInputStream
import java.io.FileNotFoundException
import java.io.IOException
import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.Date
import java.io.StringReader

object LuceneIndexer {

    // requires: index of TextAnn
  def run(outDir:String, rdd:RDD[(Long,Array[Any])], ta:Int)(implicit sc:SparkContext):Unit = {
    val t = rdd.map(x => (x._1,x._2(ta).asInstanceOf[TextAnn]))
    
    // this doesn't scale, but OK for now
    val all = t.collect
    
    val indexPath = outDir + "/luceneText"

    val dir = FSDirectory.open(new File(indexPath))
    // :Post-Release-Update-Version.LUCENE_XY:
    val analyzer = new StandardAnalyzer(Version.LUCENE_4_9)
    val iwc = new IndexWriterConfig(Version.LUCENE_4_9, analyzer)

      // Optional: for better indexing performance, if you
      // are indexing many documents, increase the RAM
      // buffer.  But if you do this, increase the max heap
      // size to the JVM (eg add -Xmx512m or -Xmx1g):
      //
      // iwc.setRAMBufferSizeMB(256.0);

    iwc.setOpenMode(OpenMode.CREATE)
    val writer = new IndexWriter(dir, iwc)

    for (x <- all)          
      indexDoc(writer, x._1.toString, x._2.text)
      
    //t.repartition(1).map(x => {
    //})
    
      // NOTE: if you want to maximize search performance,
      // you can optionally call forceMerge here.  This can be
      // a terribly costly operation, so generally it's only
      // worth it when your index is relatively static (ie
      // you're done adding documents to it):
      //
    writer.forceMerge(1);

    writer.close
  }

  def indexDoc(writer:IndexWriter, path:String, content:String) = {
    val contentReader = new StringReader(content)
    
    // make a new, empty document
    val doc = new Document

    // Add the path of the file as a field named "path".  Use a
    // field that is indexed (i.e. searchable), but don't tokenize 
    // the field into separate words and don't index term frequency
    // or positional information:
    val pathField = new StringField("path", path, Field.Store.YES)
    doc.add(pathField)

    // Add the last modified date of the file a field named "modified".
    // Use a LongField that is indexed (i.e. efficiently filterable with
    // NumericRangeFilter).  This indexes to milli-second resolution, which
    // is often too fine.  You could instead create a number based on
    // year/month/day/hour/minutes/seconds, down the resolution you require.
    // For example the long value 2011021714 would mean
    // February 17, 2011, 2-3 PM.
    //doc.add(new LongField("modified", file.lastModified, Field.Store.NO))

    // Add the contents of the file to a field named "contents".  Specify a Reader,
    // so that the text of the file is tokenized and indexed, but not stored.
    // Note that FileReader expects the file to be in UTF-8 encoding.
    // If that's not the case searching for special characters will fail.
    doc.add(new TextField("contents", contentReader))

    if (writer.getConfig.getOpenMode == OpenMode.CREATE) {
      // New index, so we just add the document (no old document can be there):
      println("adding " + path)
      writer.addDocument(doc)
    } else {
      // Existing index (an old copy of this document may have been indexed) so 
      // we use updateDocument instead to replace the old one matching the exact 
      // path, if present:
      println("updating " + path)
      writer.updateDocument(new Term("path", path), doc)
    }
  }  
  
  // finds TextAnn
  def run(outDir:String, rdd:RDD[(Long,Array[Any])])(implicit sc:SparkContext):Unit = {
    val c = firstColumnOfType(rdd, classOf[TextAnn])
    run(outDir, rdd, c)
  }

}