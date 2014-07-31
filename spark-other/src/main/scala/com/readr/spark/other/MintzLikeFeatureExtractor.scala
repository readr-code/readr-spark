package com.readr.spark.other

import java.util.Properties

import scala.collection.JavaConversions.bufferAsJavaList
import scala.collection.JavaConversions.mutableSetAsJavaSet
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
import scala.collection.mutable.HashSet
import scala.collection.mutable.Seq
import scala.collection.mutable.StringBuilder

import com.readr.model.annotation.Dependency
import com.readr.model.annotation.PosPairFeature
import com.readr.model.annotation.PosPairFeatureAnn
import com.readr.model.annotation.SentenceDependencyAnn
import com.readr.model.annotation.SentenceMentionAnn
import com.readr.model.annotation.SentenceTokenOffsetAnn
import com.readr.model.annotation.TokensAnn
import com.readr.spark.util.Annotator

object MintzLikeFeatureExtractor {

}

class MintzLikeFeatureExtractor extends Annotator(
      generates = Array(classOf[PosPairFeatureAnn]),
      requires = Array(classOf[TokensAnn],
          classOf[SentenceTokenOffsetAnn],
          classOf[SentenceDependencyAnn], //must be DepCCProcessed
          classOf[SentenceMentionAnn])) {
  
  val properties = new Properties()
  println("MINTZ")
  
  override def annotate(ins:Any*):Array[Any] = {
    Array(run(ins(0).asInstanceOf[TokensAnn], ins(1).asInstanceOf[SentenceTokenOffsetAnn], 
        ins(2).asInstanceOf[SentenceDependencyAnn], ins(3).asInstanceOf[SentenceMentionAnn]))
  }
  
  def run(ta:TokensAnn, stoa:SentenceTokenOffsetAnn, da:SentenceDependencyAnn, 
      ma:SentenceMentionAnn):PosPairFeatureAnn = {
    
	val bfss = ArrayBuffer[Array[PosPairFeature]]()
	for (sentNum <- 0 until stoa.sents.length) {
	  // get all dependencies and get all mentions, then process
			
	  val ss = stoa.sents(sentNum)
	  val tokens = ta.tokens.slice(ss.f, ss.t)
	  val deps = da.sents(sentNum)
	  val mentions = ma.sents(sentNum)
	  
	  val mentionHeads = mentions.map(x => x.headIndex).distinct

//	  val tokens = ta.tokens	  
//	  val deps = da.sents(sentNum)
//	  val mentions = ma.sents(sentNum).map(x => x.copy(headIndex = x.headIndex - ss.f))
	  
	  val bffs = ArrayBuffer[PosPairFeature]()
			
//			bffs.add(BinaryFeatureProto.Fea.newBuilder()
//					.setPos1(1).setPos2(2)
//					.setFeature("FEATUREX " + ms.getMentionsCount() + " mentions" + mentions.size() + ", " + ds.getDependenciesCount() + " deps" + deps.size()).build());

	  try {
		// for each pair of mentions...
	    val processPair:(Int,Int) => Unit = (i:Int,j:Int) => {
		  val pos1 = mentionHeads(i)
		  val pos2 = mentionHeads(j)
//		  val pos1 = mentions(i).headIndex
//		  val pos2 = mentions(j).headIndex
		  
	      // (a) mentions must be non-identical
	      if (i != j && 
		  
	      // (b) ignore if mentions are after 127 tokens in long sentence
		     (pos1 < 128 || pos2 < 128) &&
		  
		  // (c) ignore if no dependencies available for sentence
		     (!deps.isEmpty)) {
		
		    // int[] arg1Pos = { pos1, pos1+1 };
			// int[] arg2Pos = { pos2, pos2+1 };
			// if (pos1 != 17 || pos2 != 22) continue;
			// System.out.println(pos1 + " " + pos2);
		
			// convert dependencies: note: for stanford dependencies,
			// there
			// sometimes exist multiple parents; for simplicity, we only
			// consider
			// one
			// System.out.println("finding paths");
		    val paths = findPaths(deps, tokens.size, pos1, pos2) // arg1Pos, arg2Pos);
		
		    val features = pathsToFeatures(paths, tokens)
		    
		    // there may be two different paths with identical deps, leading to two identical features
		    // remove such duplicates here
		    val seen = HashSet[String]()
		    for (f <- features) {
		      val fts = f.toString
		      if (!seen.contains(fts)) {
		    	bffs += PosPairFeature(pos1, pos2, fts)
		    	seen += fts
		      }
		    }
	      }
	    }
		  
		for (i <- 0 until mentionHeads.size)
		  for (j <- 0 until mentionHeads.size)
		    processPair(i,j)

	  } catch {
		case e:Exception => e.printStackTrace
	  }
	  bfss += bffs.toArray
	}
	PosPairFeatureAnn(bfss.toArray)
  }

  private def recursive(p:UpPath, node:Int, depTo:Buffer[Buffer[Dependency]], 
      pathsToTarget:Buffer[Buffer[UpPath]]):Unit = {
    if (pathsToTarget(node).size > MAX_PATHS) return
	pathsToTarget(node) += p
	for (d <- depTo(node)) {
	  if (p.canAdd(d)) {
		val np = p.duplicate
		np.add(d)
		recursive(np, d.from, depTo, pathsToTarget)
	  }
	}
  }

  def findPaths(ld:Array[Dependency], numTokens:Int, head1:Int, head2:Int):Buffer[DoubleUpPath] = {
    
	// build an index for dependencies: from tokenPos to list of
	// dependencies pointing to this pos
	val depTo = ArrayBuffer[Buffer[Dependency]]()
	for (i <- 0 until numTokens)
	  depTo += ArrayBuffer[Dependency]()
	for (d <- ld) {
	  // ignore root
	  if (d.from >= 0)
		depTo(d.to) += d
	}

	/*
	 * // identify head words of arg1 and arg2 // (start at end, while
	 * inside entity, jump) int head1 = arg1Pos[1]-1; boolean change1 =
	 * false; do { change1 = false; for (Dependency d : depTo.get(head1)) if
	 * (d.from >= arg1Pos[0] && d.from < arg1Pos[1] && d.from != head1) {
	 * head1 = d.from; change1 = true; } } while (change1);
	 * 
	 * int head2 = arg2Pos[1]-1; boolean change2 = false; do { change2 =
	 * false; for (Dependency d : depTo.get(head2)) if (d.from >= arg2Pos[0]
	 * && d.from < arg2Pos[1] && d.from != head2) { head2 = d.from; change2
	 * = true; } } while (change2);
	 */

	// from each head we determine paths to the root
	val pathsToHead1 = ArrayBuffer[Buffer[UpPath]]()
	val pathsToHead2 = ArrayBuffer[Buffer[UpPath]]()
	for (i <- 0 until numTokens) {
	  pathsToHead1 += ArrayBuffer[UpPath]()
	  pathsToHead2 += ArrayBuffer[UpPath]()
	}

	val p = new UpPath
	recursive(p, head1, depTo, pathsToHead1)
	recursive(p, head2, depTo, pathsToHead2)

	// for debug
	// int totalPaths1 = 0;
	// int totalPaths2 = 0;
	// for (List<UpPath> ph1 : pathsToHead1) totalPaths1 += ph1.size();
	// for (List<UpPath> ph2 : pathsToHead2) totalPaths2 += ph2.size();
	//
	// // if there are too many paths, then ignore sentence
	// if ((long)totalPaths1 * (long)totalPaths2 > 1000000)
	// return new ArrayList<DoubleUpPath>();
	// System.out.println(totalPaths1 + " " + totalPaths2);

	// determine all double up paths between head1 and head2
	val dup = ArrayBuffer[DoubleUpPath]()
	for (i <- 0 until numTokens)
	  for (p1 <- pathsToHead1(i))
		for (p2 <- pathsToHead2(i)) {
		  // the two paths might be overlapping:
		  // ignore paths that have more overlaps than the root
		  if (!containsAny(p1.h, p2.d)
			&& p1.length + p2.length <= MAX_PATH_LENGTH)
			dup += DoubleUpPath(p1, p2)
		}
	dup
  }

  private def containsAny(hs:HashSet[Dependency], c:Seq[Dependency]):Boolean = {
	for (d <- c)
	  if (hs.contains(d)) return true
	return false
  }

  val MAX_PATHS = 25
  val MAX_PATH_LENGTH = 2  

  case class Feature(
	predicates:ArrayBuffer[String] = ArrayBuffer[String](),
	vg:VariableGenerator = new VariableGenerator
	) {
    
	// String arg1spanVar, arg2spanVar;

	override def toString:String = {
	  val sb = new StringBuilder
	  for (i <- 0 until predicates.size) {
		if (i > 0) sb.append(" & ")
		sb.append(predicates.get(i))
	  }
	  sb.toString
	}

	// TODO: do we need this?
	def copy:Feature = {
	  Feature(predicates.clone, vg.copy)
	}
  }

  case class VariableGenerator(var n:Int = 0) {
	
	def next:String = {
	  if (n < 26) {
	    val s = ('a'.toInt + n).toChar
	    n += 1
	    return s.toString
	  }
	  throw new Exception("no more characters to be used as variables");
	}

	def copy:VariableGenerator = {
	  val vg = new VariableGenerator(n)
	  vg.n = n
	  vg
	}
  }

  case class UpPath(
	var d:ArrayBuffer[Dependency] = ArrayBuffer[Dependency](),
	var h:HashSet[Dependency] = HashSet[Dependency]()
  ) {
    def canAdd(rd:Dependency):Boolean = !h.contains(rd)

    def add(rd:Dependency) = { d.add(rd); h.add(rd) }

    def duplicate:UpPath = {
	  val n = new UpPath();
	  n.d.addAll(d);
	  n.h.addAll(h);
      n
	}

	def length:Int = d.size
  }

  case class DoubleUpPath(p1:UpPath, p2:UpPath)
	
  private def pathsToFeatures(paths:Buffer[DoubleUpPath],
			tokens:Array[String]):Seq[Feature] = {
    val features = ArrayBuffer[Feature]()
	for (dup:DoubleUpPath <- paths;
	  // ignore long dependency paths
	  val length = dup.p1.length + dup.p2.length
	  if (length < 12)) {

	  val f = new Feature()
	  val arg1posVar:String = f.vg.next
	  val arg2posVar:String = f.vg.next
	// f.predicates.add("ent(" + f.arg1spanVar + ",EMention)");
	// f.predicates.add("ent(" + f.arg2spanVar + ",EMention)");
	// f.predicates.add("span2pos(" + f.arg1spanVar + "," + arg1posVar +
	// ")");
	// f.predicates.add("span2pos(" + f.arg2spanVar + "," + arg2posVar +
	// ")");

	  var lastPosVar = arg1posVar
	  val uplen = dup.p1.length
	  val downlen = dup.p2.length
	  for (i <- 0 until uplen) {
		val nextPosVar = if (uplen - i - 1 + downlen > 0) f.vg.next else arg2posVar
		val d:Dependency = dup.p1.d.get(i)
		f.predicates.add("dep(" + nextPosVar + ", \"" + d.name + "\", " + lastPosVar + ")")
		if (i > 0)
		  f.predicates.add("token(" + lastPosVar + ",'" +
			escape(tokens(d.to)) + "')")
		lastPosVar = nextPosVar
	  }
	  for (j <- 0 until downlen) {
		val nextPosVar = if (j < downlen - 1) f.vg.next else arg2posVar
		val d:Dependency = dup.p2.d.get(downlen - j - 1)
		f.predicates.add("dep(" + lastPosVar + ", \"" + d.name + "\", " + nextPosVar + ")")
		if (uplen + j > 0)
		  f.predicates.add("token(" + lastPosVar + ",'" +
			escape(tokens(d.from)) + "')")
		lastPosVar = nextPosVar
	  }
	  features += f
	}
	features
  }

  private def escape(s:String):String = s.replaceAll("\"", "\\\"")	
}
