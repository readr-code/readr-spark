package com.readr.spark.allenai

import org.allenai.nlpstack.postag.FactoriePostagger
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.JavaConversions._
import com.readr.model._
import com.readr.model.annotation._
import com.readr.spark.util.Annotator
import java.util._
import scala.collection.mutable.ArrayBuffer
import org.allenai.nlpstack.core.PostaggedToken
//import org.allenai.nlpstack.postag.PostaggedToken

object FactoriePOSTagger {

  //val s: Seq[org.allenai.nlpstack.postag.PostaggedToken]

  
  def toAllenai(pa:POSAnn, ta:TokensAnn, toa:TokenOffsetAnn):Seq[PostaggedToken] =
    for (i <- 0 until pa.pos.size) yield
      PostaggedToken(pa.pos(i), ta.tokens(i), toa.tokens(i).f)
 
//  def fromAllenai(seq:Iterable[Segment]):SentenceOffsetAnn =
//    SentenceOffsetAnn(seq.map(x => Offsets(x.offset, x.offset + x.text.length)).toArray)

}

class FactoriePOSTagger extends Annotator(
      generates = Array(classOf[POSAnn]),
      requires = Array(classOf[TokensAnn], classOf[TokenOffsetAnn], classOf[SentenceTokenOffsetAnn])) {
  
  @transient lazy val tagger = new FactoriePostagger
  
  override def annotate(ins:Any*):Array[Any] = {
    Array(run(ins(0).asInstanceOf[TokensAnn], ins(1).asInstanceOf[TokenOffsetAnn],
        ins(2).asInstanceOf[SentenceTokenOffsetAnn]))
  }
  
  def run(to:TokensAnn, toa:TokenOffsetAnn, soa:SentenceTokenOffsetAnn):POSAnn = {
    val n_tokens = FactorieTokenizer.toAllenai(to, toa)
    
    val tags = tagger.postagTokenized(n_tokens)
    
    POSAnn(tags.map(_.postagSymbol.name).toArray)
    
//	for (s <- soa.sents) {
//	  val sentToks = to.tokens.slice(s.f, s.t)
//	  
//	}
  }
}
