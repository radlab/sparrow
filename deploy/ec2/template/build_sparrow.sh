#!/bin/bash
# Build Sparrow locally

SPARROW_INSTALL_DIR=~/sparrow/
SPARK_INSTALL_DIR=~/spark/
SHARK_INSTALL_DIR=~/shark/

chmod 755 ~/*.sh

if [ ! -d "$SPARROW_INSTALL_DIR" ]; then
  mkdir $SPARROW_INSTALL_DIR
fi;

if [ ! -d "$SPARK_INSTALL_DIR" ]; then
  mkdir $SPARK_INSTALL_DIR
fi;

if [ ! -d "$SHARK_INSTALL_DIR" ]; then
  mkdir $SHARK_INSTALL_DIR
fi;

cd /tmp/

if [ ! -d "sparrow" ]; then
  git clone git://github.com/radlab/sparrow.git -b {{git_branch}}
fi;


if [ ! -d "spark" ]; then
  git clone git://github.com/kayousterhout/spark.git -b {{spark_git_branch}}
fi;

if [ ! -d "shark" ]; then
  git clone git://github.com/kayousterhout/shark.git -b sparrow
fi;

cd sparrow
if [ ! -e "/tmp/sparrow/target/sparrow-1.0-SNAPSHOT.jar" ]; then
  mvn package -Dmaven.test.skip=true
fi
cp /tmp/sparrow/target/sparrow-1.0-SNAPSHOT.jar $SPARROW_INSTALL_DIR

# Copy the Sparrow jar to a place where Spark includes it in the build.
if [ ! -d "/tmp/spark/core/lib" ]; then
  mkdir /tmp/spark/core/lib
fi
cp /tmp/sparrow/target/sparrow-1.0-SNAPSHOT.jar /tmp/spark/core/lib/

cd /tmp/spark
if [ ! -e "/tmp/spark/core/target/scala-2.9.1" ]; then
  sbt/sbt compile
  sbt/sbt publish-local
fi

# Manually download httpcore, which SBT seems to have issues with
wget http://repo1.maven.org/maven2/org/apache/httpcomponents/httpcore/4.1.2/httpcore-4.1.2.jar
HTTPCORE_DIR=/root/.m2/repository/org/apache/httpcomponents/httpcore/4.1.2
mkdir -p $HTTPCORE_DIR
mv httpcore-4.1.2.jar $HTTPCORE_DIR

cd /tmp/shark

# Copy shark-conv into the conf/ directory, because it's needed for building.
if [ ! -d "conf" ]; then
  mkdir conf
fi
cp ~/shark-env.sh conf/

if [ ! -e "/tmp/shark/target/scala-2.9.1/classes/shark/SharkEnv.class" ]; then
sbt/sbt products
fi
# This jar could be in sparrow spark branch instead.
cp /tmp/spark/core/lib/sparrow* /tmp/shark/lib

cp -r /tmp/spark/* $SPARK_INSTALL_DIR
cp -r /tmp/shark/* $SHARK_INSTALL_DIR
