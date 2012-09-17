#!/bin/bash
# Start Sparrow locally
ulimit -n 16384

LOG=/disk1/sparrow/sparrowDaemon.log
IP=`ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{ print $1}'`

ip_there=`cat sparrow.conf |grep hostname`
if [ "X$ip_there" == "X" ]; then
  echo "hostname = $IP" >> sparrow.conf
fi

# Make sure software firewall is stopped (ec2 firewall subsumes)
/etc/init.d/iptables stop > /dev/null 2>&1

APPCHK=$(ps aux | grep -v grep | grep -c SparrowDaemon)

if [ ! $APPCHK = '0' ]; then
  echo "Sparrow already running, cannot start it."
  exit 1;
fi

# -XX:MaxGCPauseMillis=3 
# removed nice -n -20
nohup java -XX:+UseConcMarkSweepGC -verbose:gc -XX:+PrintGCTimeStamps -Xmx2046m -XX:+PrintGCDetails -cp ./sparrow/sparrow-1.0-SNAPSHOT.jar edu.berkeley.sparrow.daemon.SparrowDaemon -c sparrow.conf > $LOG 2>&1 &
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
