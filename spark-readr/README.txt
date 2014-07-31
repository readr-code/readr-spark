The following worked:

(In each case, using SparkContext "local" to execute on local machine)

1. RUN ON LOCAL FILE

  ./run /tmp/oobelib.log


2. RUN ON HDFS FILE

- fire up hadoop-2.4.0 hdfs namenode, datanode
- then run
   ./run hdfs://localhost/test/test.log
   
3. RUN ON S3 FILE

./run s3n://readr/test.log


=========
NOW:EXECUTE 

1. local works fine


2. on local hadoop cluster??


3. on ec2/emr?

(last one not necessary, because we can run on spark cluster on ec2, see below)


=================
use spark-ec2 to fire up standalone cluster
 (can't do that on vulcan wifi network, must use tethering, due to ssh port not being let through)
 
 ./spark-ec2 -k readr -i ~/readr.pem -s 1 -z us-east-1c launch test
 
 
launching spark-shell locally with remote master didn't work for me,

but I can ssh into master node, then start spark-shell there

- ephemeral hdfs works
- persistent hdfs not launched by default (?)
   (can attach persistent ebs-volume on startup)

don't forget to run ./spark-ec2/copyDir ... on master to clone jar file(s) to all nodes
   
can now assemble and execute test program on ec2-cluster, by following steps

 scp 
 java -jar .. (executed on master)
 

=================

1. Spark Shell on local machine with spark-readr

1.1   First, fire up spark-shell, with spark-readr added to classpath

cd /Users/raphael/readr-other/spark/spark-1.0.0

bin/spark-shell --master local[7] --jars "/Users/raphael/readr/spark/spark-readr/target/spark-readr-assembly-1.0-SNAPSHOT.jar" --driver-java-options "-Dspark.serializer=org.apache.spark.serializer.KryoSerializer -Dspark.kryo.registrator=com.readr.spark.MyRegistrator"

SPARK_JAVA_OPTS="-Dspark.cores.max=<numCores>"

SPARK_MEM=8G bin/spark-shell --master local[6] --jars "/Users/raphael/readr/spark/spark-readr/target/spark-readr-assembly-1.0-SNAPSHOT.jar" --driver-java-options "-Dspark.serializer=org.apache.spark.serializer.KryoSerializer -Dspark.kryo.registrator=com.readr.spark.MyRegistrator -Dspark.kryoserializer.buffer.mb=16"



// only relevant if run on cluster
 -Dspark-cores-max=6 -Dspark.executor.memory=8000m
1.2   

//val sourceDir = "/Users/raphael/data/nyt/source2"
//val outDir = "/Users/raphael/data/nyt/out2"

val sourceDir = "/Users/raphael/data/barrons/source"
val outDir = "/Users/raphael/data/barrons/outX"


implicit val isc = sc

import com.readr.spark._
import com.readr.spark.rr._
import com.readr.spark.index._
//import com.readr.spark.stanford._
import com.readr.spark.stanfordsr._
import com.readr.spark.other._
import com.readr.spark.cj._

val a = read(sourceDir)

val b = annotate(a, new StanfordTokenizer)
val c = annotate(b, new StanfordSentenceSplitter)
//val d = annotate(c, new CJParser)
//val d = annotate(c, new StanfordParser)
val d = annotate(c, new StanfordSRParser)
val e = annotate(d, new StanfordDependencyExtractor)
val f = annotate(e, new StanfordPOSTagger)
val g = annotate(f, new StanfordLemmatizer)
val h = annotate(g, new StanfordNERTagger)
val i = annotate(h, new SimpleConstituentExtractor)
val j = annotate(i, new SimpleNERSegmenter)
val k = annotate(j, new StanfordCorefResolver)
val l = annotate(k, new SimpleMentionExtractor)
val m = annotate(l, new MintzLikeFeatureExtractor) 

m.persist

write(m, outDir)

DocumentIndexer.run(outDir, m)
SourceIndexer.run(outDir, m)
TextIndexer.run(outDir, m)
TokenIndexer.run(outDir, m)
DependencyIndexer.run(outDir, m)
NERIndexer.run(outDir, m)
PosPairFeatureIndexer.run(outDir, m)
MentionIndexer.run(outDir, m)
//DistsimIndexer.run(outDir, m)

================

val sourceDir = "/Users/raphael/data/barrons/source"
val outDir = "/Users/raphael/data/barrons/out"

val sourceDir = "/Users/raphael/data/nyt/source2"
val outDir = "/Users/raphael/data/nyt/out2"


implicit val isc = sc

import com.readr.spark._
import com.readr.spark.rr._
import com.readr.spark.index._
import com.readr.spark.stanford34._
import com.readr.spark.other._
import com.readr.spark.cj._

implicit val se = new Schema

val a = read(sourceDir, se).repartition(2)

//val b = annotate(a, new StanfordDocumentPreprocessor)
val b = annotate(a, new StanfordTokenizer, se)
val c = annotate(b, new StanfordSentenceSplitter, se)
//val d = annotate(c, new CJParser)
//val d = annotate(c, new StanfordParser)
val d = annotate(c, new StanfordSRParser, se)
val e = annotate(d, new StanfordDependencyExtractor, se)
val f = annotate(e, new StanfordPOSTagger, se)
val g = annotate(f, new StanfordLemmatizer, se)
val h = annotate(g, new StanfordNERTagger, se)
val i = annotate(h, new SimpleConstituentExtractor, se)
val j = annotate(i, new SimpleNERSegmenter, se)
val k = annotate(j, new StanfordDependencyExtractor("DepCollapsed"), se)
val l = annotate(k, new StanfordCorefResolver, se, scala.collection.immutable.Map(7 -> {se.annTyps.size-1}))
val m = annotate(l, new SimpleMentionExtractor, se)
val n = annotate(m, new MintzLikeFeatureExtractor, se) 

n.persist

write(n, outDir, se)

DocumentIndexer.run(outDir, n)
SourceIndexer.run(outDir, n)
TextIndexer.run(outDir, n)
TokenIndexer.run(outDir, n)
DependencyIndexer.run(outDir, n)
NERIndexer.run(outDir, n)
PosPairFeatureIndexer.run(outDir, n)
MentionIndexer.run(outDir, n)


======

val sourceDir = "/Users/raphael/data/nyt/source2"
val outDir = "/Users/raphael/data/nyt/out2"

implicit val isc = sc

import com.readr.spark._
import com.readr.spark.rr._
import com.readr.spark.index._
import com.readr.spark.stanford34._
import com.readr.spark.other._
import com.readr.spark.cj._

implicit val se = new Schema

val a = read(sourceDir, se).repartition(2)

//val b = annotate(a, new StanfordDocumentPreprocessor)
val b = annotate(a, new StanfordTokenizer, se)
val c = annotate(b, new StanfordSentenceSplitter, se)
val d = annotate(c, new StanfordSRParser, se)
//val d = annotate(c, new CJParser)
//val d = annotate(c, new StanfordParser)
val n = d

n.persist

write(n, outDir, se)

=====================


val sourceDir = "/Users/raphael/data/nyt/out2"
val outDir = "/Users/raphael/data/nyt/out3"

implicit val isc = sc

import com.readr.spark._
import com.readr.spark.rr._
import com.readr.spark.index._
import com.readr.spark.stanford34._
import com.readr.spark.other._
import com.readr.spark.cj._

implicit val se = new Schema

val l = read(sourceDir, se)

val m = annotate(l, new SimpleMentionExtractor, se)
val n = annotate(m, new MintzLikeFeatureExtractor, se) 

n.persist

write(n, outDir, se)

===

n.first._2(n.first._2.size-1).asInstanceOf[com.readr.model.annotation.PosPairFeatureAnn].sents(0)(4)
n.first._2(n.first._2.size-1).asInstanceOf[com.readr.model.annotation.PosPairFeatureAnn].sents(0)(3)



case class RSchema(
  Array[Class[_]] // classOf
)

case class RDDwSchema (
  rdd:RDD[
  schema:Array[
)

val k = annotate(j, new StanfordCorefResolver)


val (d1, s1) = annotate(d1, s1, new StanfordPOSTagger)
val (d2, s2) = annotate(d2, s2, new StanfordCorefResolver)





val a1 = a.repartition(6)


old stuff:
import com.readr.spark._
import com.readr.spark.rr._

val s = rr.read(sc, "/users/raphael/test2")
   
val t = rr.annotate(sc, s, new com.readr.spark.stanford.StanfordTokenizer, 1)
   
val u = rr.annotate(sc, t, new com.readr.spark.stanford.StanfordSentenceSplitter, 1, 2, 3, 4)

val v = rr.annotate(sc, u, new com.readr.spark.stanford.StanfordParser, 1, 5, 3, 4)
   
val w = rr.annotate(sc, v, new com.readr.spark.stanford.StanfordDependencyExtractor, 1, 5, 3, 4, 7)

val dependencyAnnID = 8


    Array(run(ins(0).asInstanceOf[TextAnn], 
        ins(1).asInstanceOf[SentenceOffsetAnn], 
        ins(2).asInstanceOf[TokenOffsetAnn], 
        ins(3).asInstanceOf[TokensAnn], 
        ins(4).asInstanceOf[ParseAnn]))



v.first._2(7).asInstanceOf[com.readr.model.annotation.ParseAnn].sents   
   
   
   t.first
   
   rr.write(sc, t, "/users/raphael/test3")


import com.readr.model.annotation._
val t = s.map(x => (x._1,x._2(0).asInstanceOf[TextAnn]))

  def escape(str:String):String = {
    val sb = new StringBuilder
    for (i <- 0 until str.length) {
      val c = str.charAt(i)
      if (c == '\t') sb.append("\\t")
      else if (c == '\n') sb.append("\\n")
      else if (c == '\\') sb.append("\\\\")
      else sb.append(c)
    }
    sb.toString    
  }

val u = t.map(x => "" + x._1 + "\t" + escape(x._2.text))

u.saveAsTextFile("/users/raphael/test4/textu")





rdd.flatMap(x => {
  val b = ArrayBuffer[String]()
  val sentenceTokenOffsetAnn = x._2(6).asInstanceOf[SentenceTokenOffsetAnn]
  val mentionAnn = x._2(15).asInstanceOf[MentionAnn]
  for (i <- 0 until mentionAnn.sents.size) {
     val s = mentionAnn.sents(i)
     val off = sentenceTokenOffsetAnn.sents(i)
     for (m <- s)
        if (m.headIndex > off.t-off.f)
        //if (m.headIndex < off.f || m.headIndex >= off.t)
            b += m.headIndex.toString + " not in " + off
    }
  b
}).take(100)


