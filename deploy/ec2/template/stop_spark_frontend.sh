#!/bin/bash
# Stop spark frontend locally

APPCHK=$(ps aux | grep -v grep | grep -v stop | grep -c spark)

if [ $APPCHK = '0' ]; then
  echo "Spark is not running. Doing nothing."
  exit 0;
fi
ps -ef |grep spark | grep -v stop | grep -v grep | awk '{ print $2; }' | xargs -I {} kill -9 {}
echo "Stopped spark process"
exit 0;
