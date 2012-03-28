#!/bin/bash
# Start mesos master

# Make sure software firewall is stopped (ec2 firewall subsumes)
/etc/init.d/iptables stop > /dev/null 2>&1

APPCHK=$(ps aux | grep -v grep | grep -c mesos-master)

if [ ! $APPCHK = '0' ]; then
  echo "Mesos master already running, cannot start it."
  exit 1;
fi

LOG=/disk1/sparrow/mesosMaster.log
nohup /opt/mesos/bin/mesos-master > $LOG 2>&1 &
PID=$!
echo "Logging to $LOG"
sleep 1
if ! kill -0 $PID > /dev/null 2>&1; then
  echo "Mesos masger failed to start"
  exit 1;
else
  echo "Mesos master started with pid $PID"
  exit 0;
fi
