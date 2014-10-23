[![Build Status](https://api.shippable.com/projects/540e79223479c5ea8f9ea41c/badge?branchName=master)](https://app.shippable.com/projects/540e79223479c5ea8f9ea41c/builds/latest)

# readr-spark

Natural language processing library for Apache Spark. It offers the following features:

* Wraps Stanford CoreNLP, CJ Parser, Allenai PolyParser, and much more
* All pluggable through a common data model
* Easy preview of outputs
* Easy scaling through Spark
* Annotator outputs stored as columns
* Incremental processing of annotations
* Efficient serialization through Kryo

In addition, it offers easy connectivity with the [Readr](http://readr.com) cloud tool:

* Indices needed by Readr cloud computed in spark, and bulk loaded into Readr cloud
* Also includes computation for Readr interface features, such as text similarity
* Can be used in combination with [readr-connect](http://github.com/readr-code/readr-connect)

### Usage

We assume you have sbt 0.13 or higher installed. Start by creating an assembly for readr-spark and directories for our inputs and outputs.

```
mkdir $HOME/readr
cd $HOME/readr
git clone https://github.com/readr-code/readr-spark.git
cd readr-spark
sbt assembly
mkdir in
mkdir out
```

This will create a file `main/target/scala-2.10/main-assembly-1.1-SNAPSHOT.jar` that contains the readr-spark with all dependent jars.

Next, you convert your data into a format readable by readr-spark. Create a new sbt project and add the following to `build.sbt`.

```scala
libraryDependencies ++= Seq(
  "com.readr" % "model" % "1.1-SNAPSHOT",
  "com.readr" % "client" % "1.1-SNAPSHOT",
)
resolvers ++= Seq(
  "Readr snapshots" at "http://snapshots.mvn-repo.readr.com",
  "Readr releases" at "http://releases.mvn-repo.readr.com"
)
```

You can now write your text documents as follows:

```scala
import org.apache.hadoop.conf.Configuration
import com.readr.model.annotation._
import com.readr.client.util.AnnotationSequenceFileWriter

object WriteInput {
  def main(args:Array[String]) {
    val inDir = System.getenv("HOME") + "/readr/in"
    val conf = new Configuration()

    val w = new AnnotationSequenceFileWriter(conf, inDir + "/data.col0.TextAnn")
    for (clazz <- Annotations.annWithDependentClazzes) w.register(clazz)

    w.write(0, TextAnn("This is the first document."))
    w.write(1, TextAnn("This is the second document."))

    w.close
  }
}
```

Make sure you have [Spark](http://spark.apache.org) installed. From the Spark directory, can now start the spark-shell and run a series of processors on our data.

```
SPARK_MEM=4G bin/spark-shell --master local[2] --jars "$HOME/readr/readr-spark/main/target/scala-2.10/main-assembly-1.1-SNAPSHOT.jar" --driver-java-options "-Dspark.serializer=org.apache.spark.serializer.KryoSerializer -Dspark.kryo.registrator=com.readr.spark.MyRegistrator -Dspark.kryoserializer.buffer.mb=16"
```

Copy and paste the following commands into spark shell:

```scala
val inDir = System.getenv("HOME") + "/readr/in"
val outDir = System.getenv("HOME") + "/readr/out"

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

val a = read(inDir, se).repartition(2)

val b = annotate(a, new FactorieSegmenter, se)
val c = annotate(b, new FactorieTokenizer, se)
val d = annotate(c, new FactoriePOSTagger, se)
val e = annotate(d, new MorphaStemmer, se)
val f = annotate(e, new PolyParser, se)
val g = annotate(f, new com.readr.spark.allenai.SimpleMentionExtractor, se)
val n = g

n.persist

write(n, outDir, se)
```

You can also view annotations in the spark-shell; for example, `f.first._1` contains the document id for the first document and `f.first._2` an array of all annotations up to the PolyParser annotations. `f.first._2(7)` returns the sentence dependency annotations. 

After the files have been written, you can read them in code as follows:

```scala
import org.apache.hadoop.conf.Configuration
import com.readr.model.annotation._
import com.readr.client.util.AnnotationSequenceFileReader

object ReadOutput {
  def main(args:scala.Array[String]):Unit = {
    val outDir = System.getenv("HOME") + "/readr/out"
    val conf = new Configuration()
    val r = new AnnotationSequenceFileReader(conf,
      Array(classOf[TextAnn], classOf[SentenceDependencyAnn]),
      outDir + "/data.col0.TextAnn",
      outDir + "/data.col7.SentenceDependencyAnn")
    for (clazz <- Annotations.annWithDependentClazzes) r.register(clazz)

    var t:scala.Tuple2[Long,scala.Array[Any]] = null

    while ({ t = r.read; t != null} ) {
      val id = t._1
      val ta = t._2(0).asInstanceOf[TextAnn]
      val sda = t._2(1).asInstanceOf[SentenceDependencyAnn]
      println(ta)
      println(sda)
    }
    r.close
  }
}
```

For more information on how to connect with Readr cloud, see these [examples](http://github.com/readr-code/readr-connect). You can build the indexes for Readr cloud as follows

```scala
DocumentIndexer.run(outDir, n)
SourceIndexer.run(outDir, n)
TextIndexer.run(outDir, n)
TokenIndexer.run(outDir, n)
DependencyIndexer.run(outDir, n)
POSIndexer.run(outDir, n)
LemmaIndexer.run(outDir, n)
```

