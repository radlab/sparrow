#!/usr/bin/env bash

# Set Spark environment variables for your site in this file. Some useful
# variables to set are:
# - MESOS_HOME, to point to your Mesos installation
# - SCALA_HOME, to point to your Scala installation
SCALA_HOME=/opt/scala-2.9.3
SPARK_CLASSPATH="/root/spark/core/lib/sparrow-1.0-SNAPSHOT.jar"

# Add Shark jars
SHARK_HOME="/root/shark"
SPARK_CLASSPATH+=:$SHARK_HOME/target/scala-$SCALA_VERSION/classes
for jar in `find $SHARK_HOME/lib -name '*jar'`; do
  SPARK_CLASSPATH+=:$jar
done
for jar in `find $SHARK_HOME/lib_managed/jars -name '*jar'`; do
  SPARK_CLASSPATH+=:$jar
done
for jar in `find $SHARK_HOME/lib_managed/bundles -name '*jar'`; do
  SPARK_CLASSPATH+=:$jar
done

# Add Hive jars.
export HIVE_HOME="/opt/hive/build/dist"
for jar in `find $HIVE_HOME/lib -name '*jar'`; do
  # Ignore the logging library since it has already been included with the Spark jar.
  if [[ "$jar" != *slf4j* ]]; then
    SPARK_CLASSPATH+=:$jar
  fi
done

# Add location of Hive configuration file.
SPARK_CLASSPATH+=:/opt/hive/conf/

SPARK_JAVA_OPTS="-Dspark.io.compression.codec=spark.io.LZFCompressionCodec -Dspark.local.dir=/disk1/spark-tmp"
# - SPARK_MEM, to change the amount of memory used per node (this should
#   be in the same format as the JVM's -Xmx option, e.g. 300m or 1g).
# - SPARK_LIBRARY_PATH, to add extra search paths for native libraries.


