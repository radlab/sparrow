#!/bin/bash
cat /disk1/sparrow/spark_* |grep waitT > /root/spark_`hostname -i`_waits.log
touch foo.log && gzip -f *.log
