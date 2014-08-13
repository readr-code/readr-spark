
bin/spark-shell --master local[4] --executor-memory 3g --driver-memory 3g --driver-class-path "/Users/raphael/readr-spark/spark-netlib/target/scala-2.10/spark-netlib-assembly-1.0-SNAPSHOT.jar" --jars "/Users/raphael/readr-spark/spark-readr/target/scala-2.10/spark-readr-assembly-1.0-SNAPSHOT.jar,/Users/raphael/readr-spark/spark-distsim/target/scala-2.10/spark-distsim-assembly-1.0-SNAPSHOT.jar" --driver-java-options "-Dspark.serializer=org.apache.spark.serializer.KryoSerializer -Dspark.kryo.registrator=com.readr.spark.MyRegistrator -Dspark.kryoserializer.buffer.mb=512 -Dcom.github.fommil.netlib.BLAS=com.github.fommil.netlib.NativeRefBLAS Dspark.executor.memory=4G"


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
import com.readr.spark.distsim._

implicit val se = new Schema

val a = read(sourceDir, se).repartition(8)

val b = annotate(a, new FactorieSegmenter, se)
val c = annotate(b, new FactorieTokenizer, se)
c.persist()
LSAOnSentences.run(outDir, c)
