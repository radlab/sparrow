#!/bin/bash
# Generate databases

SCALE=$1
FRONTENDS=`cat frontends.txt`

if [ "$SCALE" = "" ];
then
  echo "Scale factor required"
  exit -1
fi

if [ -d "/disk1/tpch" ];
then
  rm -rf /disk1/tpch
fi

mkdir /disk1/tpch
cp /opt/tpch/dbgen/* /disk1/tpch

for fe in $FRONTENDS; do
  echo "Generating database for $fe"
  cd /disk1/tpch
  ./dbgen -f -s $SCALE
  fe=`dig +short $fe`
  mkdir $fe
  cp *.tbl $fe
  sudo -u hdfs /opt/hadoop/bin/hadoop dfs -rmr "hdfs://`hostname`:8020/$fe/*"
  sudo -u hdfs /opt/hadoop/bin/hadoop dfs -rmr "hdfs://`hostname`:8020/$fe/"
  sudo -u hdfs /opt/hadoop/bin/hadoop dfs -mkdir "hdfs://`hostname`:8020/$fe/"
  sudo -u hdfs /opt/hadoop/bin/hadoop dfs -Ddfs.block.size=33554432 -copyFromLocal $fe/*.tbl hdfs://`hostname`:8020/$fe/
  sudo -u hdfs /opt/hadoop/bin/hadoop dfs -chmod -R 777 "hdfs://`hostname`:8020/$fe/"
  rm -rf $fe
  cd -
done
