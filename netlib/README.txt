Using the native netlib libraries with spark requires an assembly of
com.github.fommil.netlib:all:1.1.2 that is not loaded with --jars but
with --driver-class-path (so that it is available to the right classloader).

This project creates this netlib assembly. Use as follows:

bin/spark-shell --driver-class-path ".../target/scala-2.10/spark-distsim-assembly-1.0-SNAPSHOT.jar" ...
