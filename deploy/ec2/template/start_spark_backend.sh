#!/bin/bash
# Start Spark backend
ulimit -n 16384
FRONTENDS=`cat frontends.txt`


APPCHK=$(ps aux | grep -v grep | grep spark |grep -c java)

if [ ! $APPCHK = '0' ]; then
  echo "Spark already running, cannot start it."
  exit 1;
fi

port=8300
for fe in $FRONTENDS; do
  ip=`dig +short $fe`
  id=spark_$ip
  log=/disk1/sparrow/$id
  chmod 755 spark-run.sh
  HOSTNAME=`ec2metadata  | grep local-hostname  | cut -d " " -f 2`
  export SPARK_HOSTNAME=$HOSTNAME

  ./spark-run.sh -Dspark.scheduler=sparrow -Dspark.master.port=7077 -Dspark.hostname=$HOSTNAME -Dspark.master.host=$ip -Dsparrow.app.name=$id -Dsparrow.app.port=$port spark.executor.SparrowExecutorBackend > $log 2>&1 &
  ((port++))
  PID=$!
  echo "Logging to $log"
  sleep .5
  if ! kill -0 $PID > /dev/null 2>&1; then
    echo "Spark executor failed to start"
    exit 1;
  else
    echo "Spark executor started with pid $PID"
  fi
done
