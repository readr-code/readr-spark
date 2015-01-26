package com.readr.spark.unindex


import com.readr.model.Offsets
import com.readr.model.annotation.{FrameMatchFeatureArg, FrameMatchFeature, TokenOffsetAnn}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object FrameMatchFeatureUnindexer {

  // groups by document
  def run(frameMatchFeaturesFile:String, frameMatchFeaturesArgFlatFile:String)(implicit sc:SparkContext):RDD[(Long,Array[Any])] = {
    run1(frameMatchFeaturesFile, frameMatchFeaturesArgFlatFile)
    //fmfs.groupBy
  // TODO
    return null
  }

    // doesn't group by document
  def run1(frameMatchFeaturesFile:String, frameMatchFeaturesArgFlatFile:String)(implicit sc:SparkContext):RDD[FrameMatchFeature] = {
    val rdd1 = sc.textFile(frameMatchFeaturesFile)
      .map(x => {
        val cols = x.split("\t")
        (java.lang.Long.parseLong(cols(0)), cols)})
    val rdd2 = sc.textFile(frameMatchFeaturesArgFlatFile)
      .map(x => {
        val cols = x.split("\t")
        (java.lang.Long.parseLong(cols(0)), cols)})
    val fmfs = rdd1.join(rdd2).map(
      _ match { case (id,(x,y)) =>
        FrameMatchFeature(instanceID = id.toInt, frameID = Integer.parseInt(x(1)), truth = x(2).equals("1"),
          priority = Integer.parseInt(x(3)),
          args = {
            val a = y(2).split(" ")
            for (i <- 0 until a.size) yield
              FrameMatchFeatureArg(argNum = i.toByte,
                documentID = Integer.parseInt(y(1)), Integer.parseInt(a(i))) })
      })
    fmfs
      //Tuple2(java.lang.Long.parseLong(cols(0)), Array(TokenOffsetAnn(offsets)))
  }
}
