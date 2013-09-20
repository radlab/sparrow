#!/bin/bash
# Starts an experiment to test scheduler throughput with spark.
# Starts the master and slaves, and then the throughpupt
# tester.
my_ip=`hostname -i`
log="/disk1/sparrow/spark_throughput_$my_ip.log"

# Spark needs the list of backends in order to start them all.
cp ~/backends.txt /root/spark/conf/slaves
/root/spark/bin/start-all.sh

# Give the master and workers a moment to get started.
echo "Done starting master and workers; sleeping before starting tester"
sleep 5s
echo "Starting throughput tester"

/root/spark/run spark.scheduler.sparrow.ThroughputTester "spark://`hostname`:7077" {{total_cores}} 60000 10000,1000,500,100 > $log 2>&1 &

