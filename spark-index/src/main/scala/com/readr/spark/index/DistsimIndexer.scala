package com.readr.spark.index

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

import com.readr.model._
import com.readr.model.annotation._
import com.readr.spark.util.Utils._


object DistsimIndexer {
  
  // cosine similarity
  def vectorSimilarity(set1:Iterable[(String,Int)], set2:Iterable[(String,Int)]):Double = {
    val m1 = new HashMap[String,Int]()
    var sum1 = 0
    val it1 = set1.iterator
    while (it1.hasNext) {
      val n = it1.next
      val other = n._1
      var count = n._2
      sum1 += count*count
      m1 += other -> count
    }
    var dot = 0
    var sum2 = 0
    val it2 = set2.iterator
    while (it2.hasNext) {
      val n = it2.next
      val other = n._1
      var count = n._2
      sum2 += count*count
      val c1 = m1.get(other)
      if (c1.isDefined)
        dot += c1.get * count
    }    
    val denom = Math.sqrt(sum1.toDouble) * Math.sqrt(sum2.toDouble)
    val r = if (denom == 0) java.lang.Double.NEGATIVE_INFINITY else dot / denom
	r
  }  
  
  val stopwords = HashSet("a", "about", "above", "above", "across", "after", "afterwards", "again", "against", "all", "almost", "alone", "along", "already", "also","although","always","am","among", "amongst", "amoungst", "amount",  "an", "and", "another", "any","anyhow","anyone","anything","anyway", "anywhere", "are", "around", "as",  "at", "back","be","became", "because","become","becomes", "becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond", "bill", "both", "bottom","but", "by", "call", "can", "cannot", "cant", "co", "con", "could", "couldnt", "cry", "de", "describe", "detail", "do", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven","else", "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fify", "fill", "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from", "front", "full", "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how", "however", "hundred", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its", "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must", "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own","part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since", "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still", "such", "system", "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thickv", "thin", "third", "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves", "the")
  
  def isValidToken1(name:String):Boolean = {
    val ch = name.charAt(0)
	if (!Character.isLetter(ch)) 
	  return false
	if (stopwords.contains(name))
	  return false
    true
  }
  
  def isValidToken2(name:String, count:Long):Boolean = {
	// minimum 2 occurrences
	if (count <= 1)
	  false
	else
	  true
  }
  
  val window = 5
  
  def run(outDir:String, rdd:RDD[(Long,Array[Any])], textAnnID:Int, tokenOffsetAnnID:Int)
  	(implicit sc:SparkContext):Unit = {
    
    val neighbors = rdd.flatMap(x => {
      val l = new ArrayBuffer[((String,String),Int)]
      val textAnn = x._2(textAnnID).asInstanceOf[TextAnn]
      val tokenOffsetAnn = x._2(tokenOffsetAnnID).asInstanceOf[TokenOffsetAnn]
      val tokens = tokenOffsetAnn.tokens.map(x => textAnn.text.substring(x.f, x.t))
      val filteredTokens = tokens.filter(isValidToken1)
      for (i <- 0 until filteredTokens.size)
        for (j <- Math.max(0, i - window) until Math.min(filteredTokens.size-1, i + window)) {
          if (i != j)
            l += Tuple2(Tuple2(filteredTokens(i), filteredTokens(j)), 1)
        }
      l
    })
    
    // ((String,String), Int) => (String, (String, Int))
    val counted = neighbors.reduceByKey((c1, c2) => c1 + c2).map(x => Tuple2(x._1._1, Tuple2(x._1._2, x._2)))
    
    val grouped = counted.groupByKey()
    
    def cmp(v1:(String,(String,Double)), v2:(String,(String,Double))):Boolean = {
      v1._2._2 > v2._2._2
    }

    
    val pairs = grouped.cartesian(grouped).map(x => {
      val token1 = x._1._1
      val vector1 = x._1._2
      val token2 = x._2._1
      val vector2 = x._2._2
//      Tuple2(Tuple2(token1, - vectorSimilarity(vector1, vector2)), token2)
//    }).filter(x => x._1._2 < 0).sortByKey()
    
      Tuple2(token1, Tuple2(token2, vectorSimilarity(vector1, vector2)))
    }).filter(x => x._2._2 > 0).groupBy(_._1).map(x => {
      // iterable
      val l = x._2.toList
      val sorted = l.sortWith(cmp)
      val topK = sorted.slice(0, Math.min(50, sorted.size))
      val str = topK.map(x => x._2._1 + " " + x._2._2).mkString(",")
      (x._1, str)
    })
    
    pairs.map(tsv(_)).saveAsTextFile(outDir + "/distsim")
    
    
//    /*
// * Generates 1 file:
// * distsim : { id1, id2, sim }
// */
//-- grunt commands
//-- copyFromLocal ...
//-- A = LOAD '/users/raphael/test/token2name' AS (id:int, name:chararray);
//-- B = LOAD '/users/raphael/test/tokenInst2basic' AS (id:int, sentenceID:int, pos:int);
//-- REGISTER '/users/raphael/readr/modules/process-distsim-pig/target/process-distsim-pig-1.0-SNAPSHOT.jar'; 
//
//
//A = LOAD '$dir/db/token2name' AS (id:int, name:chararray);
//B = LOAD '$dir/db/tokenInst2basic' AS (id:int, sentenceID:int, pos:int);
//
//-- determine valid tokens (remove stop words etc.)
//C = JOIN A BY $0, B BY $0;
//D = GROUP C BY ($0,$1);
//E = FOREACH D GENERATE flatten($0), COUNT($1);
//F = FILTER E BY com.readr.hadoop.module.distsimIndex.ValidToken($0,$1,$2);
//F1 = FOREACH F GENERATE $0;
//
//-- generate pairs
//B0 = JOIN B BY $0, F1 BY $0;
//G = GROUP B0 BY $1;
//H = FOREACH G GENERATE flatten(com.readr.hadoop.module.distsimIndex.ExtractPairs($1));
//I = GROUP H BY ($0,$1);
//J = FOREACH I GENERATE FLATTEN($0),COUNT($1);
//K = GROUP J BY $0;
//L = FOREACH K {
//  MA = ORDER J BY $2 DESC;
//  MB = LIMIT MA 1000;
//  GENERATE $0, MB;
//};
//  -- todo: shouldn we sort and cutoff by normalized counts?
//M = FOREACH L GENERATE $0,$1;
//N = CROSS L, M;
//O = FOREACH N GENERATE $0 AS id1, $2 AS id2, com.readr.hadoop.module.distsimIndex.VectorSimilarity($1, $3) as sim;
//P = GROUP O BY $0;
//Q = FOREACH P {
//  RA = ORDER O BY $2 DESC;
//  RB = LIMIT RA 100;
//  GENERATE FLATTEN(RB);
//};
//R = ORDER Q BY $0 ASC, $2 DESC;
//
//-- store
//STORE R INTO '$dir/db/distsim';    
  }
  
  
  def run(outDir:String, rdd:RDD[(Long,Array[Any])])(implicit sc:SparkContext):Unit = {
    val c0 = firstColumnOfType(rdd, classOf[TextAnn])
    val c1 = firstColumnOfType(rdd, classOf[TokenOffsetAnn])
    run(outDir, rdd, c0, c1)
  }  
}
