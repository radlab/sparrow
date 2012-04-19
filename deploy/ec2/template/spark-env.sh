#!/usr/bin/env bash

# Set Spark environment variables for your site in this file. Some useful
# variables to set are:
# - MESOS_HOME, to point to your Mesos installation
# - SCALA_HOME, to point to your Scala installation
SPARK_CLASSPATH="core/lib/mesos.jar:core/target/spark-core-assembly-0.4-SNAPSHOT.jar"
SPARK_JAVA_OPTS=""
# - SPARK_MEM, to change the amount of memory used per node (this should
#   be in the same format as the JVM's -Xmx option, e.g. 300m or 1g).
# - SPARK_LIBRARY_PATH, to add extra search paths for native libraries.


