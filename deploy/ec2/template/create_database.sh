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
  cp *.tbl $fe
  sudo -u hdfs /opt/hadoop/bin/hadoop dfs -rmr "hdfs://`hostname`:8020/$fe/*"
  sudo -u hdfs /opt/hadoop/bin/hadoop dfs -rmr "hdfs://`hostname`:8020/$fe/"
  sudo -u hdfs /opt/hadoop/bin/hadoop dfs -mkdir "hdfs://`hostname`:8020/$fe/"
  # Copy each table to a directory for this frontend
  for t in *.tbl; do
    name=`echo $t | sed "s/.tbl//g"`
    sudo -u hdfs /opt/hadoop/bin/hadoop dfs -mkdir "hdfs://`hostname`:8020/$fe/$name/"
    sudo -u hdfs /opt/hadoop/bin/hadoop dfs -Ddfs.block.size=33554432 -copyFromLocal $t hdfs://`hostname`:8020/$fe/$name/
  done
  sudo -u hdfs /opt/hadoop/bin/hadoop dfs -chmod -R 777 "hdfs://`hostname`:8020/$fe/"
  cd -
done

echo "Making Hive User Directory"
sudo -u hdfs /opt/hadoop/bin/hadoop dfs -mkdir "hdfs://`hostname`:8020/user/"
sudo -u hdfs /opt/hadoop/bin/hadoop dfs -mkdir "hdfs://`hostname`:8020/user/hive/"
sudo -u hdfs /opt/hadoop/bin/hadoop dfs -chmod -R 777 "hdfs://`hostname`:8020/user/"

echo "Making Temp Dir"
sudo -u hdfs /opt/hadoop/bin/hadoop dfs -mkdir "hdfs://`hostname`:8020/tmp/"
sudo -u hdfs /opt/hadoop/bin/hadoop dfs -chmod -R 777 "hdfs://`hostname`:8020/tmp/"
