[![Build Status](https://api.shippable.com/projects/53f2a2d7bc562cba01744b67/badge/master)](https://www.shippable.com/projects/53f2a2d7bc562cba01744b67)
# Readr Spark API

This API makes it easy to run NLP pipelines on Apache Spark and create indices to be used with [Readr](http://readr.com).

### Create readr-spark assembly

`sbt assembly`

### Convert own data into Readr format

see CreateSource

### Run Annotators in spark shell

SPARK_MEM=4G bin/spark-shell --master local[2] --jars "...../readr-spark/target/scala-2.10/spark-readr-assembly-1.0-SNAPSHOT.jar" --driver-java-options "-Dspark.serializer=org.apache.spark.serializer.KryoSerializer -Dspark.kryo.registrator=com.readr.spark.MyRegistrator -Dspark.kryoserializer.buffer.mb=16"

Now you can run a few annotators

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


If you are interested in the annotations created, you can view them as follows:

