#!/bin/bash
# Start Prototype backend

LOG=/disk1/sparrow/protoBackend

APPCHK=$(ps aux | grep -v grep | grep -c ProtoBackend)

if [ ! $APPCHK = '0' ]; then
  echo "Backend already running, cannot start it."
  exit 1;
fi

nohup java -cp ./sparrow/sparrow-1.0-SNAPSHOT.jar edu.berkeley.sparrow.examples.ProtoBackend  > $LOG 2>&1 &
PID=$!
echo "Logging to $LOG"
sleep 1
if ! kill -0 $PID > /dev/null 2>&1; then
  echo "Proto backend failed to start"
  exit 1;
else
  echo "Proto backend started with pid $PID"
  exit 0;
fi
