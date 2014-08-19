//package com.readr.spark.distsim
//
//import org.apache.spark.SparkContext
//import org.apache.spark.SparkContext._
//import org.apache.spark.rdd._
//import scala.collection.mutable.HashSet
//import scala.collection.JavaConversions._
//import com.readr.model._
//import com.readr.model.annotation._
//import com.readr.model.frame._
//import com.readr.spark.util.Annotator
//import scala.collection.mutable.ArrayBuffer
//import java.util.Properties
//import com.readr.spark.util.Utils._
//import org.apache.spark.mllib.linalg.Matrix
//import org.apache.spark.mllib.linalg.distributed.RowMatrix
//import org.apache.spark.mllib.linalg.Vector
//import org.apache.spark.mllib.linalg.Vectors
//import org.apache.spark.mllib.linalg.SingularValueDecomposition
//import org.apache.spark.mllib.linalg.DenseVector
//
//
//object Use {
//  
//  val dir = "/Users/raphael/data/processed/barrons-4th-grade"
//  
//  def main(args:Array[String]) = {
//    
//  }
//  
//  def tt = {
//    // load matrices into memory
//    
//    val docRefs = ArrayBuffer[(Int,Int)]()
//    val singVals = new DenseVector(new Double[])
//    val leftVecs = ArrayBuffer[DenseVector]()
//    val rightVecs = ArrayBuffer[DenseVector]()
//    
//    // TODO: part files ...
//    {
//      val r = new BufferedReader(new InputStreamReader(new FileInputStream(dir + "/svd_singular_values")))
//      var l:String = null
//      while ({ l = r.readLine; l != null} ) {
//      
//      }
//    }
//    
//    
//    {
//      val r = new BufferedReader(new InputStreamReader(new FileInputStream(dir + "/svd_left_singular_vectors")))
//      var l:String = null
//      while ({ l = r.readLine; l != null} ) {
//      
//      }
//    }
//      
//    
//  }
//  
//  def termSimilarity(i:Int, j:Int):Double = {
//    0.0
//  }
//  
//  def docSimilarity(i:Int, j:Int):Double = {
//    0.0
//  }
//
//  def newDocSimilarity(i:Int, j:Int):Double = {
//    0.0
//  }
//  
//}