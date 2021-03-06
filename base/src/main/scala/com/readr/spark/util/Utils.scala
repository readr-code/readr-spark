package com.readr.spark.util

import org.apache.spark.rdd.RDD

object Utils {
 
  def firstColumnOfType(rdd:RDD[(Long,Array[Any])], c:Class[_]):Int = {
    // TODO: what if RDD is empty
    val f = rdd.first
    val a = f._2
    for (i <- 0 until a.length)
      if (a(i).getClass.equals(c)) return i
    -1
  }  

  def escape(str:String):String = {
    val sb = new StringBuilder
    for (i <- 0 until str.length) {
      val c = str.charAt(i)
      if (c == '\t') sb.append("\\t")
      else if (c == '\n') sb.append("\\n")
      else if (c == '\\') sb.append("\\\\")
      else sb.append(c)
    }
    sb.toString    
  }

  def unescape(str:String):String = {
    val sb = new StringBuilder

    var i = 0
    while (i < str.length) {
      val c = str.charAt(i)
      if (c == '\\') {
        val n = str.charAt(i+1)
        if (n == 'n')
          sb.append('\n')
        else if (n == 't')
          sb.append('\t')
        else if (n == '\\')
          sb.append('\\')
        i += 1
      } else
        sb.append(c)
      i += 1
    }
    sb.toString
  }

  def tsv(pr:Product) = {
    val sb = new StringBuilder
    for (i <- 0 until pr.productArity) {
      if (i > 0) sb.append("\t")
      sb.append(pr.productElement(i))
    }
    sb.toString
  }

}