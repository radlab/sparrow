#!/bin/bash
# Stop sparrow locally

APPCHK=$(ps aux | grep -v grep | grep -c SparrowDaemon)

if [ $APPCHK = '0' ]; then
  echo "Sparrow is not running. Doing nothing."
  exit 0;
fi
ps -ef |grep SparrowDaemon |grep -v grep | awk '{ print $2; }' | xargs -I {} kill -9 {}
echo "Stopped Sparrow process"
exit 0;
