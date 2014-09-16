//package com.readr.spark
//
//import com.readr.spark.util.Annotator
//import com.typesafe.config.{Config, ConfigFactory}
//import org.apache.spark._
//import scala.util.Try
//import java.util.{Random, Date}
//
//import spark.jobserver._
//
//object ReadrJob extends SparkJob {
//
//  def main(args: Array[String]) {
//    val sc = new SparkContext("local[4]", "ReadrJob")
//    val config = ConfigFactory.parseString("")
//    val results = runJob(sc, config)
//    println("Result is " + results)
//  }
//
//  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
//    Try(config.getString("input.dir"))
//      .map(x => SparkJobValid)
//      .getOrElse(SparkJobInvalid("No input.dir config param"))
//    Try(config.getString("input.annotator"))
//      .map(x => SparkJobValid)
//      .getOrElse(SparkJobInvalid("No input.annotator config param"))
//
//  }
//
//  override def runJob(sc: SparkContext, config: Config): Any = {
//    val sourceDir = config.getString("input.dir")
//    var annotatorName = config.getString("input.annotator")
//
//    // instantiate annotator
//
//    import rr._
//
//    implicit val isc = sc
//
//    implicit val se = new Schema
//
//    val a = read(sourceDir, se).repartition(2)
//
//    val annotator = instantiate[Annotator](annotatorName)
//
//    val b = annotate(a, annotator, se)
//
//    write(b, sourceDir + ".out", se)
//
//  }
//
//   // input: dir, annotator
//   // or: dir, annotator, indexes/names for argument mapping
//   // output: nothing, but job writes additional annotations to dir, (outputs filenames?)
//
//   // job then writes
//  // read ==>
//  // val b = annotate(
//
//
//
//  private def instantiate[T <: Annotator](annotatorName:String):T = {
//    val clazz = Class.forName(annotatorName)
//    val instance = clazz.newInstance.asInstanceOf[T]
//    val initializable = instance.asInstanceOf[{ def init: Unit }]
//    initializable.init
//    instance
//  }
//}
