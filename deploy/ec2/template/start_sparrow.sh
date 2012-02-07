#!/bin/bash
# Start Sparrow locally

LOG=/tmp/sparrowDaemon.log

# Make sure software firewall is stopped (ec2 firewall subsumes)
/etc/init.d/iptables stop > /dev/null 2>&1

APPCHK=$(ps aux | grep -v grep | grep -c SparrowDaemon)

if [ ! $APPCHK = '0' ]; then
  echo "Sparrow already running, cannot start it."
  exit 1;
fi

nohup java -cp ./sparrow/sparrow-1.0-SNAPSHOT.jar edu.berkeley.sparrow.daemon.SparrowDaemon -c sparrow.conf > $LOG 2>&1 &
PID=$!
echo "Logging to $LOG"
sleep 1
if ! kill -0 $PID > /dev/null 2>&1; then
  echo "Sparrow Daemon failed to start"
  exit 1;
else
  echo "Sparrow Daemon started with pid $PID"
  exit 0;
fi
