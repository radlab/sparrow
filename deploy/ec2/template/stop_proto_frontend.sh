#!/bin/bash
# Stop proto frontend locally

APPCHK=$(ps aux | grep -v grep | grep -c ProtoFrontend)

if [ $APPCHK = '0' ]; then
  echo "ProtoFrontend is not running. Doing nothing."
  exit 0;
fi
ps -ef |grep ProtoFrontend |grep -v grep | awk '{ print $2; }' | xargs -I {} kill -9 {}
echo "Stopped ProtoFrontend process"
exit 0;
