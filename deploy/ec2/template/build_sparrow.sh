#!/bin/bash
# Build Sparrow locally

INSTALL_DIR=~/sparrow/

if [ ! -d "$INSTALL_DIR" ]; then
  mkdir $INSTALL_DIR
fi;

cd /tmp/

if [ ! -d "sparrow" ]; then
  git clone git://github.com/radlab/sparrow.git -b {{git_branch}}
fi;

cd sparrow
if [ ! -e "/tmp/sparrow/target/sparrow-1.0-SNAPSHOT.jar" ]; then
  mvn package -Dmaven.test.skip=true
fi
cp /tmp/sparrow/target/sparrow-1.0-SNAPSHOT.jar $INSTALL_DIR
