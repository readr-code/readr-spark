package com.readr.spark.unindex

import com.readr.model.Offsets
import com.readr.model.annotation.{TokenOffsetAnn, TextAnn}
import com.readr.spark.util.Utils._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object TokenUnindexer {

  def run(documentTokenOffsetsFile:String)(implicit sc:SparkContext):RDD[(Long,Array[Any])] = {
    val rdd = sc.textFile(documentTokenOffsetsFile)
    rdd.map(x => {
      val cols = x.split("\t")
//      val offsets = cols(1).split(" ").flatMap(x => {
//          val pair = x.split(":")
//          if (pair.size == 2)
//            Array(Offsets(Integer.parseInt(pair(0)), Integer.parseInt(pair(1))))
//          else Array[Offsets]()
//        })
      println(x)
      println(cols(1))
      val offsets = cols(1).split(" ").map(z => {
        val pair = z.split(":")
        Offsets(Integer.parseInt(pair(0)), Integer.parseInt(pair(1)))
      })
      Tuple2(java.lang.Long.parseLong(cols(0)), Array(TokenOffsetAnn(offsets)))
    })
  }
}
