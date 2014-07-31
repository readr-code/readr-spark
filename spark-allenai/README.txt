
SPARK_MEM=4G bin/spark-shell --master local[2] --jars "/Users/raphael/readr/spark/spark-readr/target/spark-readr-assembly-1.0-SNAPSHOT.jar" --driver-java-options "-Dspark.serializer=org.apache.spark.serializer.KryoSerializer -Dspark.kryo.registrator=com.readr.spark.MyRegistrator -Dspark.kryoserializer.buffer.mb=16"


val sourceDir = "/Users/raphael/data/source/barrons-4th-grade"
val outDir = "/Users/raphael/data/processed/barrons-4th-grade"

implicit val isc = sc

import com.readr.spark._
import com.readr.spark.rr._
import com.readr.spark.index._
import com.readr.spark.allenai._
import com.readr.spark.stanford34._
import com.readr.spark.other._
import com.readr.spark.cj._
import com.readr.spark.frame._

implicit val se = new Schema

val a = read(sourceDir, se).repartition(2)

val b = annotate(a, new FactorieSegmenter, se)
val c = annotate(b, new FactorieTokenizer, se)
val d = annotate(c, new FactoriePOSTagger, se)
val e = annotate(d, new MorphaStemmer, se)
val f = annotate(e, new PolyParser, se)
val g = annotate(f, new com.readr.spark.allenai.SimpleMentionExtractor, se)
val n = g

n.persist

write(n, outDir, se)

DocumentIndexer.run(outDir, n)
SourceIndexer.run(outDir, n)
TextIndexer.run(outDir, n)
TokenIndexer.run(outDir, n)
DependencyIndexer.run(outDir, n)
POSIndexer.run(outDir, n)
LemmaIndexer.run(outDir, n)
MentionIndexer.run(outDir, n)
//NERIndexer.run(outDir, n)
//LuceneIndexer.run(outDir, n)


import com.readr.model.annotation.FrameMatchFeature
import com.readr.model.frame.Frame

val q = sc.emptyRDD[(Long,Frame,Seq[FrameMatchFeature])]

val (s,r) = MentionFrameCreator.run(q, n)

FrameIndexer.run(outDir, s, r)



val r = extend(q, new MentionFrameCreator, n)
val s = extend(




val e1 = annotate(d, new StanfordSRParser, se)
val f = annotate(e1, new StanfordPOSTagger, se)
val g = annotate(f, new StanfordLemmatizer, se)
val h = annotate(g, new StanfordNERTagger, se)
val i = annotate(h, new SimpleConstituentExtractor, se)
val j = annotate(i, new SimpleNERSegmenter, se)
val k = annotate(j, new StanfordDependencyExtractor("DepCollapsed"), se)
val l = annotate(k, new StanfordCorefResolver, se, scala.collection.immutable.Map(7 -> {se.annTyps.size-1}))
val m = annotate(l, new SimpleMentionExtractor, se)
val n = annotate(m, new MintzLikeFeatureExtractor, se) 

NERIndexer.run(outDir, n)
PosPairFeatureIndexer.run(outDir, n)
MentionIndexer.run(outDir, n)
