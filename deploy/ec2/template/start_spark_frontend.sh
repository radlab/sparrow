#!/bin/bash
# Start Prototype frontend

APPCHK=$(ps aux | grep -v grep |grep -v start| grep java | grep -c spark)
SCHED=$1
MESOS_MASTER=`cat frontends.txt | head -n 1`

/etc/init.d/iptables stop > /dev/null 2>&1

if [ "$SCHED" = "" ];
then
  echo "Scheduler required"
  exit -1
fi

if [ ! $APPCHK = '0' ]; then
  echo "Frontend already running, cannot start it."
  exit 1;
fi

ip=`ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{ print $1}'`
log="/disk1/sparrow/spark_$ip.log"

chmod 755 spark-run.sh

if [ "$SCHED" = "sparrow" ];
then
  export SPARK_HOSTNAME=`hostname`.ec2.internal
  nohup ./spark-run.sh -Dspark.master.host=$ip -Dspark.master.port=7077 -Dsparrow.app.name=spark_$ip -Dspark.scheduler=sparrow spark.SparkTPCHRunner sparrow@localhost:20503 hdfs://{{name_node}}:8020/ 1 true 1 60 /root/spark_tpch_$ip.log > $log 2>&1 &
else
  SPARK_MEM="3000m"
  export SPARK_MEM
  nohup ./spark-run.sh -Dspark.master.host=$ip -Dspark.master.port=7077 -Dspark.locality.wait=500000 spark.SparkTPCHRunner master@$MESOS_MASTER:5050 hdfs://{{name_node}}:8020/ 1 true 1 60 /root/spark_tpch_$ip.log > $log 2>&1 &
fi

PID=$!
echo "Logging to $log"
sleep 1
if ! kill -0 $PID > /dev/null 2>&1; then
  echo "Proto frontend failed to start"
  exit 1;
else
  echo "Proto frontend started with pid $PID"
  exit 0;
fi
