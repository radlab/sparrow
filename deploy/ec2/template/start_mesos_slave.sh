#!/bin/bash
# Start mesos slave

# Make sure software firewall is stopped (ec2 firewall subsumes)
/etc/init.d/iptables stop > /dev/null 2>&1

APPCHK=$(ps aux | grep -v grep | grep -c mesos-slave)

export MESOS_HOME="/opt/mesos"

CPUS=`grep processor /proc/cpuinfo | wc -l`
#CPUS=8
MEM_KB=`cat /proc/meminfo | grep MemTotal | awk '{print $2}'`
MEM=$[(MEM_KB - 1024 * 1024) / 1024] # Leaves 1 GB free
RES_OPTS="--resources=cpus:$CPUS;mem:$MEM"

if [ ! $APPCHK = '0' ]; then
  echo "Mesos slave already running, cannot start it."
  exit 1;
fi
MASTER=`cat frontends.txt | head -n 1`
LOG=/disk1/sparrow/mesosSlave.log
HOSTNAME=`ec2metadata  | grep local-hostname  | cut -d " " -f 2`
export SPARK_HOSTNAME=$HOSTNAME

nohup /opt/mesos/bin/mesos-slave $RES_OPTS --master=mesos://master@$MASTER:5050> $LOG 2>&1 &
PID=$!
echo "Logging to $LOG"
sleep 1
if ! kill -0 $PID > /dev/null 2>&1; then
  echo "Mesos slave failed to start"
  exit 1;
else
  echo "Mesos slave started with pid $PID"
  exit 0;
fi
