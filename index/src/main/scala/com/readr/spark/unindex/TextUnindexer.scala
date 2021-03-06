package com.readr.spark.unindex

import com.readr.model.annotation.TextAnn
import com.readr.spark.util.Utils
import com.readr.spark.util.Utils._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object TextUnindexer {

  def run(textsFile:String)(implicit sc:SparkContext):RDD[(Long,Array[Any])] = {
    val rdd = sc.textFile(textsFile)
    rdd.map(x => {
      val cols = x.split("\t")
      Tuple2(java.lang.Long.parseLong(cols(0)), Array(TextAnn(Utils.unescape(cols(1)))))
    })
  }
}
