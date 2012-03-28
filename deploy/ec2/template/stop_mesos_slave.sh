#!/bin/bash
# Stop mesos slave

APPCHK=$(ps aux | grep -v grep | grep -c mesos-slave)

if [ $APPCHK = '0' ]; then
  echo "Mesos slave is not running. Doing nothing."
  exit 0;
fi
ps -ef |grep mesos-slave| grep -v grep | awk '{ print $2; }' | xargs -I {} kill -9 {}
echo "Stopped mesos slave process."
exit 0;
