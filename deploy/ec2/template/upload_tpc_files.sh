#!/bin/bash
dir=$1
sudo -u hdfs /opt/hadoop/bin/hadoop dfs -Ddfs.block.size=33554432 -copyFromLocal /opt/tpch/dbgen/$dir/*.tbl hdfs://`hostname`:8020/

