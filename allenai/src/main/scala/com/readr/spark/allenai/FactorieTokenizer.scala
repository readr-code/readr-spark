package com.readr.spark.allenai

import scala.collection.mutable.ArrayBuffer

import com.readr.model.Offsets
import com.readr.model.annotation.SentenceOffsetAnn
import com.readr.model.annotation.SentenceTokenOffsetAnn
import com.readr.model.annotation.TextAnn
import com.readr.model.annotation.TokenOffsetAnn
import com.readr.model.annotation.TokensAnn
import com.readr.spark.util.Annotator

object FactorieTokenizer {
  def toAllenai(to:TokensAnn, soa:TokenOffsetAnn):Seq[org.allenai.nlpstack.core.Token] =
    for (i <- 0 until to.tokens.length) yield new org.allenai.nlpstack.core.Token(to.tokens(i), soa.tokens(i).f)

  def fromAllenai(seq:Iterable[org.allenai.nlpstack.core.Token]):TokenOffsetAnn =
    TokenOffsetAnn(seq.map(x => Offsets(x.offset, x.offset + x.string.length)).toArray)  
}

class FactorieTokenizer extends Annotator(
      generates = Array(classOf[TokenOffsetAnn], classOf[TokensAnn], classOf[SentenceTokenOffsetAnn]),
      requires = Array(classOf[TextAnn], classOf[SentenceOffsetAnn])) {
  
  @transient lazy val tokenizer = new org.allenai.nlpstack.tokenize.FactorieTokenizer
  
  override def annotate(ins:Any*):Array[Any] = {
    val t = run(ins(0).asInstanceOf[TextAnn],
        ins(1).asInstanceOf[SentenceOffsetAnn])
    Array(t._1, t._2, t._3)
  }
  
  def run(t:TextAnn, soa:SentenceOffsetAnn):(TokenOffsetAnn, TokensAnn, SentenceTokenOffsetAnn) = {
    
    val l_toa = new ArrayBuffer[Offsets]()
    val l_ta = new ArrayBuffer[String]()
    val l_stoa = new ArrayBuffer[Offsets]()
    
    var lastToken = 0
    for (so <- soa.sents) {
      val sent = t.text.substring(so.f, so.t)
      
      val tokens = tokenizer.tokenize(sent)

      l_toa.appendAll(tokens.map(x => Offsets(so.f + x.offset, so.f + x.offset + x.string.length)))
      
      l_ta.appendAll(tokens.map(_.string))
      
      l_stoa.append(Offsets(lastToken, lastToken + tokens.size))
      
      lastToken += tokens.size
      //l_stoa.append(tokens.map(x => Offsets(x.offset, x.offset + x.string.length)).toArray)
    }
    Tuple3(TokenOffsetAnn(l_toa.toArray), TokensAnn(l_ta.toArray), SentenceTokenOffsetAnn(l_stoa.toArray))
  }
}
