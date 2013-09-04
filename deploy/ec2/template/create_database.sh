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

echo "Generating database"
cd /disk1/tpch
./dbgen -f -s $SCALE


echo "Copying database"
sudo -u hdfs /opt/hadoop/bin/hadoop dfs -rmr "hdfs://{{name_node}}:8020/tpch/*"
sudo -u hdfs /opt/hadoop/bin/hadoop dfs -rmr "hdfs://{{name_node}}:8020/tpch/"
sudo -u hdfs /opt/hadoop/bin/hadoop dfs -mkdir "hdfs://{{name_node}}:8020/tpch/"

# Make and populate /tpch/[tbl] directory for each table
for t in *.tbl; do
  name=`echo $t | sed "s/.tbl//g"`
  sudo -u hdfs /opt/hadoop/bin/hadoop dfs -mkdir "hdfs://{{name_node}}:8020/tpch/$name/"
  sudo -u hdfs /opt/hadoop/bin/hadoop dfs -Ddfs.block.size=33554432 -copyFromLocal $t hdfs://{{name_node}}:8020/tpch/$name/
done

echo "Making Hive User Directory"
sudo -u hdfs /opt/hadoop/bin/hadoop dfs -mkdir "hdfs://{{name_node}}:8020/user/"
sudo -u hdfs /opt/hadoop/bin/hadoop dfs -mkdir "hdfs://{{name_node}}:8020/user/hive/"

echo "Making Temp Dir"
sudo -u hdfs /opt/hadoop/bin/hadoop dfs -mkdir "hdfs://{{name_node}}:8020/tmp/"
sudo -u hdfs /opt/hadoop/bin/hadoop dfs -chmod -R 777 "hdfs://{{name_node}}:8020/tmp/"

echo "Making Hive warehouse dir for denorm table"
sudo -u hdfs /opt/hadoop/bin/hadoop dfs -mkdir "hdfs://{{name_node}}:8020/tpch/denorm-warehouse/"

