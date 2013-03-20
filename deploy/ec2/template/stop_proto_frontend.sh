#!/bin/bash
# Stop proto frontend locally

APPCHK=$(ps aux | grep -v grep | grep -c {{frontend_type}})

if [ $APPCHK = '0' ]; then
  echo "{{frontend_type}} is not running. Doing nothing."
  exit 0;
fi
ps -ef |grep {{frontend_type}} |grep -v grep | awk '{ print $2; }' | xargs -I {} kill -9 {}
echo "Stopped {{frontend_type}} process"
exit 0;
