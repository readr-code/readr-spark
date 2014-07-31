package com.readr.spark.malt

import com.readr.model.annotation.SentenceOffsetAnn
import com.readr.model.annotation.SentenceTokenOffsetAnn
import scala.collection.mutable.ArrayBuffer
import com.readr.model.annotation.TokenOffsetAnn
import com.readr.model.Offsets

object Helpers {

  def getSentenceTokenOffsetAnn(sep:SentenceOffsetAnn, toa:TokenOffsetAnn):SentenceTokenOffsetAnn = {
    val tokens = toa.tokens
	
	val stoas = new ArrayBuffer[Offsets]()
	var nextTok = 0
	for (i <- 0 until sep.sents.size) {
	  val s = sep.sents(i)
			
	  var beginTok = -1
	  var endTok = -1
	  
	  while (nextTok < tokens.size && tokens(nextTok).f < s.f) nextTok += 1
	  beginTok = nextTok
	  endTok = beginTok
	  while (endTok < tokens.size && tokens(endTok).t <= s.t) endTok += 1
	  nextTok = endTok
	  
	  stoas += Offsets(beginTok, endTok)
	}
    SentenceTokenOffsetAnn(stoas.toArray)
  }

}