#!/bin/bash

# TODO put this in image
#apt-get install -y ntp
#/etc/init.d/ntp start 

rm -rf /tmp/spark-local*

if [ -d "/mnt" ]; then
  umount /mnt/
  rmdir /mnt/
fi;

if [ ! -d "/disk1" ]; then
  mkdir /disk1
  mount /dev/xvdb /disk1 -t ext3
fi;

mkdir -p /disk1/hdfs/name
chown hdfs.hdfs /disk1/hdfs/name

mkdir -p /disk1/sparrow
mkdir -p /disk1/hdfs/data
mkdir -p /disk1/tmp/
mkdir -p /disk1/tmp/spark/
chown hdfs.hdfs /disk1/hdfs/data

# Annoying ec2 cloud entry in hostfile
cat /etc/hosts | grep -v internal > tmp && mv tmp /etc/hosts

cp ~/hdfs-site.xml /opt/hadoop/conf/
cp ~/hive-default.xml /opt/hive/conf/

# Create references to per-frontend hive tables
cd /opt/tpch_hive/
bash -c "sed -i 's/\/tpch/\/`hostname -i`/g' *.hive"
cd -
