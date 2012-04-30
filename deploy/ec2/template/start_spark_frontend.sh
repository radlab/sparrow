#!/bin/bash
# Start Prototype frontend
ulimit -n 16384

APPCHK=$(ps aux | grep -v grep |grep -v start| grep java | grep -c spark)
SCHED=$1
RATE=$2
MAX=$3
QNUM=$4
PAR_LEVEL=$5
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
HOSTNAME=`ec2metadata  | grep local-hostname  | cut -d " " -f 2`

if [ "$SCHED" = "sparrow" ];
then
  export SPARK_HOSTNAME=$HOSTNAME
  nohup ./spark-run.sh -Dspark.master.host=$ip -Dspark.master.port=7077 -Dsparrow.app.name=spark_$ip -Dspark.scheduler=sparrow spark.SparkTPCHRunner sparrow@localhost:20503 hdfs://{{name_node}}:8020/`hostname -i`/ $QNUM true $RATE $MAX /root/spark_tpch_$ip.log $PAR_LEVEL > $log 2>&1 &
else
  SPARK_MEM="1300m"
  export SPARK_MEM
  nohup ./spark-run.sh -Dspark.master.host=$ip -Dspark.master.port=7077 -Dspark.locality.wait=500000 spark.SparkTPCHRunner master@$MESOS_MASTER:5050 hdfs://{{name_node}}:8020/`hostname -i`/ $QNUM true $RATE $MAX /root/spark_tpch_$ip.log $PAR_LEVEL > $log 2>&1 &
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
