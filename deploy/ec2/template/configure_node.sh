#!/bin/bash

/etc/init.d/ntpd stop
ntpdate ntp.ubuntu.com

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
mkdir -p /disk1/spark-tmp
mkdir -p /disk1/hdfs/data
mkdir -p /disk1/tmp/
mkdir -p /disk1/tmp/spark/
chown hdfs.hdfs /disk1/hdfs/data

# Annoying ec2 cloud entry in hostfile
cat /etc/hosts | grep -v internal > tmp && mv tmp /etc/hosts

cp ~/hdfs-site.xml /opt/hadoop/conf/
cp ~/hadoop-env.sh /opt/hadoop/conf/
cp ~/backends.txt /opt/hadoop/conf/slaves
cp ~/hive-site.xml /opt/hive/conf/
cp ~/shark-env.sh /root/shark/conf/shark-env.sh
cp ~/spark-env.sh /root/spark/conf/spark-env.sh

# Create references to per-frontend hive tables
cd /opt/tpch_hive/
bash -c "sed -i 's/\/tpch/\/`hostname -i`/g' *.hive"
cd -

# Reference correct directory in hive queries
bash -c "sed -i 's/HOST_IP/`hostname -i`/g' ~/tpch/*"

