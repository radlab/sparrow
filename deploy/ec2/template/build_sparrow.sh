#!/bin/bash
# Build Sparrow locally

INSTALL_DIR=~/sparrow/

if [ ! -d "$INSTALL_DIR" ]; then
  mkdir $INSTALL_DIR
fi;

cd /tmp/

# Remove old install if here
if [ -d "sparrow" ]; then
  rm -rf sparrow
fi;

git clone git://github.com/radlab/sparrow.git -b {{git_branch}}
cd sparrow
mvn package -Dmaven.test.skip=true
cp /tmp/sparrow/target/sparrow-1.0-SNAPSHOT.jar $INSTALL_DIR
