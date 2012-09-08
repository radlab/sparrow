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
  git clone git://github.com/pwendell/spark.git -b {{spark_git_branch}}
fi;

if [ ! -d "shark" ]; then
  git clone git://github.com/pwendell/shark.git -b sparrow
fi;

cd sparrow
if [ ! -e "/tmp/sparrow/target/sparrow-1.0-SNAPSHOT.jar" ]; then
  mvn package -Dmaven.test.skip=true
fi
cp /tmp/sparrow/target/sparrow-1.0-SNAPSHOT.jar $SPARROW_INSTALL_DIR

cd /tmp/spark
if [ ! -e "/tmp/spark/core/target/scala-2.9.1" ]; then
  sbt/sbt compile
  sbt/sbt publish-local
fi

cd /tmp/shark
#if [ ! -e "/tmp/shark/bin/shark" ]; then
sbt/sbt products
#fi
# This jar could be in sparrow spark branch instead.
cp /tmp/spark/core/lib/sparrow* /tmp/shark/lib

cp -r /tmp/spark/* $SPARK_INSTALL_DIR
cp -r /tmp/shark/* $SHARK_INSTALL_DIR
