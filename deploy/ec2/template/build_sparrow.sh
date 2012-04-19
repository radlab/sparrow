#!/bin/bash
# Build Sparrow locally

SPARROW_INSTALL_DIR=~/sparrow/
SPARK_INSTALL_DIR=~/spark/

chmod 755 ~/*.sh

if [ ! -d "$SPARROW_INSTALL_DIR" ]; then
  mkdir $SPARROW_INSTALL_DIR
fi;

if [ ! -d "$SPARK_INSTALL_DIR" ]; then
  mkdir $SPARK_INSTALL_DIR
fi;

cd /tmp/

if [ ! -d "sparrow" ]; then
  git clone git://github.com/radlab/sparrow.git -b {{git_branch}}
fi;


if [ ! -d "spark" ]; then
  git clone git://github.com/pwendell/spark.git -b {{spark_git_branch}}
fi;

cd sparrow
if [ ! -e "/tmp/sparrow/target/sparrow-1.0-SNAPSHOT.jar" ]; then
  mvn package -Dmaven.test.skip=true
fi
cp /tmp/sparrow/target/sparrow-1.0-SNAPSHOT.jar $SPARROW_INSTALL_DIR

cd /tmp/spark
sbt/sbt compile
cp -r /tmp/spark/* $SPARK_INSTALL_DIR
