#!/bin/bash
# Start shark tpch workload
ulimit -n 16384

APPCHK=$(ps aux | grep -v grep |grep -v start| grep java | grep -c spark)

ip=`ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{ print $1}'`
log="/disk1/sparrow/shark_$ip.log"

public_hostname=`ec2metadata  | grep public-hostname  | cut -d " " -f 2`
fe_num=`cat frontends.txt | grep -n $public_hostname | cut -d ":" -f 1`

cd /disk1/sparrow/
sleep .0$[ ( $RANDOM % 10 ) + 1 ]s # Helps avoid synchronization
/root/shark/run shark.sparrow.SparrowTPCHRunner /root/tpch/tpch_workload_$fe_num {{inter_query_delay}} > $log 2>&1 &

PID=$!
echo "Logging to $log"
sleep 1
if ! kill -0 $PID > /dev/null 2>&1; then
  echo "Shark TPCH failed to start"
  exit 1;
else
  echo "Shark TPCH started with pid $PID"
  exit 0;
fi
