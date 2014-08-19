package com.readr.spark.distsim

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.collection.mutable.HashSet
import scala.collection.JavaConversions._
import com.readr.model._
import com.readr.model.annotation._
import com.readr.model.frame._
import com.readr.spark.util.Annotator
import scala.collection.mutable.ArrayBuffer
import java.util.Properties
import com.readr.spark.util.Utils._
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import org.apache.spark.mllib.linalg.DenseVector
import org.khelekore.prtree.PRTree
import java.awt.geom.Rectangle2D
import org.khelekore.prtree.junit.TestPRTree.RectDistance
import org.khelekore.prtree.SimplePointND
import java.util.Collections
import org.khelekore.prtree.MBRConverter
import org.khelekore.prtree.DistanceCalculator
import org.khelekore.prtree.PointND
import org.khelekore.prtree.MinDist2D
import org.khelekore.prtree.NodeFilter
import java.util.ArrayList


object PRTreeTest {
  
  val dir = "/Users/raphael/data/processed/barrons-4th-grade"

  class Rectangle2DConverter extends MBRConverter[Rectangle2D] {
	def getDimensions = 2

	def getMin (axis:Int, t:Rectangle2D):Double =
	    if (axis == 0) t.getMinX() else t.getMinY()

	def getMax (axis:Int, t:Rectangle2D):Double =
	    if (axis == 0) t.getMaxX() else t.getMaxY()
  }    
  
  class RectDistance extends DistanceCalculator[Rectangle2D] {
	def distanceTo(r:Rectangle2D, p:PointND):Double = {
	    val md = MinDist2D.get (r.getMinX (), r.getMinY (),
				       r.getMaxX (), r.getMaxY (),
				       p.getOrd (0), p.getOrd (1))
	    return Math.sqrt (md)
	}
  }
  
  class AcceptAll[T] extends NodeFilter[T] {
	def accept (t:T) = true
  }  
    
  
  class SimplePointNDConverter extends MBRConverter[SimplePointND] {
    def getDimensions = 5
	def getMin(axis:Int, t:SimplePointND) = t.getOrd(axis)
	def getMax(axis:Int, t:SimplePointND) = t.getOrd(axis)    
  }

  class SPDistance extends DistanceCalculator[SimplePointND] {
	def distanceTo(r:SimplePointND, p:PointND):Double = {
	  val arr = for (i <- 0 until r.getDimensions) yield { (r.getOrd(i) - p.getOrd(i))*(r.getOrd(i) - p.getOrd(i)) }
	  val dis = arr.reduce(_ + _)
	  dis
	}
  }
  
  def main(args:Array[String]) = {
    val acceptAllFilter = new AcceptAll[SimplePointND]()
    
    val tree = new PRTree(new SimplePointNDConverter, 30)
    val arr = new ArrayList[SimplePointND]()
    arr.add(new SimplePointND(1,2,3,4,5))
    arr.add(new SimplePointND(3,2,3,4,5))
    arr.add(new SimplePointND(6,2,3,4,5))
    tree.load(arr)
    
    val dc = new SPDistance()
    val p = new SimplePointND(2, 3,3,4,5)
    val maxHits = 5
    val nnRes = tree.nearestNeighbour (dc, acceptAllFilter, maxHits, p)
    println ("Nearest neighbours are: " + nnRes)    
    
    for (dr <- nnRes) {
      println(dr.getDistance())
    }
  }
  
    
  def main2(args:Array[String]) = {
   
    val acceptAllFilter = new AcceptAll[Rectangle2D]()
    
    val tree = new PRTree[Rectangle2D](new Rectangle2DConverter (), 30)
    val rx = new Rectangle2D.Double (0, 0, 1, 1)
    tree.load (Collections.singletonList (rx))
    for (r:Rectangle2D <- tree.find (0, 0, 1, 1)) {
      System.out.println ("found a rectangle: " + r);
    }

    val dc = new RectDistance()
    val p = new SimplePointND(2, 3)
    val maxHits = 5
    val nnRes = tree.nearestNeighbour (dc, acceptAllFilter, maxHits, p)
    println ("Nearest neighbours are: " + nnRes)    
    
    for (dr <- nnRes) {
      println(dr.getDistance())
    }
  }
}