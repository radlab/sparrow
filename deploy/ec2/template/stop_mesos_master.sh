#!/bin/bash
# Stop mesos master

APPCHK=$(ps aux | grep -v grep | grep -c mesos-master)

if [ $APPCHK = '0' ]; then
  echo "Mesos master is not running. Doing nothing."
  exit 0;
fi
ps -ef |grep mesos-master| grep -v grep | awk '{ print $2; }' | xargs -I {} kill -9 {}
echo "Stopped mesos master process."
exit 0;
