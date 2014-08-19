package com.readr.spark.allenai

import scala.collection.JavaConversions._
import org.apache.spark.SparkContext._
import com.readr.model._
import com.readr.model.annotation._
import com.readr.spark.util.Annotator
import org.allenai.nlpstack.core.Segment

object FactorieSegmenter {
  
  def toAllenai(ta:TextAnn, soa:SentenceOffsetAnn):Seq[Segment] =
    soa.sents.map(x => Segment(ta.text.substring(x.f, x.t), x.f))

  def fromAllenai(seq:Iterable[Segment]):SentenceOffsetAnn =
    SentenceOffsetAnn(seq.map(x => Offsets(x.offset, x.offset + x.text.length)).toArray)
}

class FactorieSegmenter extends Annotator(
      generates = Array(classOf[SentenceOffsetAnn]),
      requires = Array(classOf[TextAnn])) {
  
  @transient lazy val segmenter = new org.allenai.nlpstack.segment.FactorieSegmenter
  
  override def annotate(ins:Any*):Array[Any] = {
    Array(run(ins(0).asInstanceOf[TextAnn]))
  }
  
  def run(t:TextAnn):SentenceOffsetAnn = {    
    val segments = segmenter.segment(t.text)    
    FactorieSegmenter.fromAllenai(segments)
  }
}
