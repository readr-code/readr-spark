package com.readr.spark.unindex

import com.readr.model.Offsets
import com.readr.model.annotation.{Mention, MentionAnn, TokenOffsetAnn, TextAnn}
import com.readr.spark.util.Utils._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions

object MentionUnindexer {

  // documentID, mentionNum, head, start, end, mentionTyp, number, gender, animacy
  def run(mentionFile:String)(implicit sc:SparkContext):RDD[(Long,Array[Any])] = {
    val rdd = sc.textFile(mentionFile)
    val rdd2 = rdd.map(x => {
      val cols = x.split("\t")

      import java.lang.{Byte,Long}
      val documentID = Long.parseLong(cols(0))
      val mentionNum = Integer.parseInt(cols(1))
      val head = Integer.parseInt(cols(2))
      val tokenOffsets = Offsets(Integer.parseInt(cols(3)), Integer.parseInt(cols(4)))
      val mentionTyp = Byte.parseByte(cols(5))
      val number = Byte.parseByte(cols(6))
      val gender = Byte.parseByte(cols(7))
      val animacy = Byte.parseByte(cols(8))

      val mention = Mention(mentionNum, head, tokenOffsets, mentionTyp, number, gender, animacy)

      Tuple2(documentID, mention)
    })
    val rdd3 = new PairRDDFunctions(rdd2).groupByKey().map(x => {
      Tuple2(x._1, Array(MentionAnn(x._2.toArray):Any))
    })
    rdd3
  }
}
