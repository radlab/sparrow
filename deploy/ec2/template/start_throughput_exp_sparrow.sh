#!/bin/bash
# Starts an experiment to test scheduler throughput.
my_ip=`hostname -i`
log="/disk1/sparrow/spark_$my_ip.log"
# for sparrow master: "sparrow@$my_ip:20503

/root/spark/run -Dspark.serializer=spark.KryoSerializer -Dsparrow.app.name=spark_`hostname -i` -Dspark.broadcast.port=33624 -Dspark.driver.port=60500 spark.scheduler.sparrow.ThroughputTester "sparrow@$my_ip:20503" {{total_cores}} 10000,1000,500,100 > $log 2>&1 &
