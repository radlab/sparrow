#!/usr/bin/env bash

# Copyright (C) 2012 The Regents of The University California.
# All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# (Required) Amount of memory used per slave node. This should be in the same
# format as the JVM's -Xmx option, e.g. 300m or 1g.
export SPARK_MEM=15g

# (Required) Set the master program's memory
export SHARK_MASTER_MEM=1g

# (Required) Point to your Scala installation.
export SCALA_HOME="/opt/scala-2.9.3"
export SCALA_VERSON=2.9.3
# Use this for non-HVM amis:
export JAVA_HOME="/usr/lib/jvm/java-6-sun/jre"
# Use this for HVM amis:
#export JAVA_HOME="/usr/lib/jvm/jre-1.6.0-openjdk.x86_64"

# (Required) Point to the patched Hive binary distribution
export HIVE_DEV_HOME="/opt/hive"
export HIVE_HOME="$HIVE_DEV_HOME/build/dist"

# (Optional) Specify the location of Hive's configuration directory. By default,
# it points to $HIVE_HOME/conf
export HIVE_CONF_DIR="$HIVE_DEV_HOME/conf"

# For running Shark in distributed mode, set the following:
export HADOOP_HOME="/opt/hadoop/"
export SPARK_HOME="/disk1/tmp/spark/"

my_ip=`hostname -i`
before_me=`cat /root/sparrow_schedulers.txt | grep -B 1000 $my_ip | grep -v $my_ip | tr "\n" ","`
after_me=`cat /root/sparrow_schedulers.txt | grep -A 1000 $my_ip | grep -v $my_ip | tr "\n" ","`
export MASTER="sparrow@$my_ip:20503,"
# Temp fix: need to add this back to the master thing for fault tolerance $before_me$after_me"

# Only required if using Mesos:
#export MESOS_NATIVE_LIBRARY=/usr/local/lib/libmesos.so 

# (Optional) Extra classpath
#export SPARK_LIBRARY_PATH=""

# Java options
# On EC2, change the local.dir to /mnt/tmp
SPARK_JAVA_OPTS="-Dspark.local.dir=/tmp "
SPARK_JAVA_OPTS+="-Dspark.serializer=spark.KryoSerializer -Dspark.kryoserializer.buffer.mb=10 "
SPARK_JAVA_OPTS+="-verbose:gc -XX:-PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseConcMarkSweepGC"
SPARK_JAVA_OPTS+=" -Dsparrow.app.name=spark_`hostname -i`"
# Spark options that are usually sent to the executor using environment variables packaged
# from the Spark master, but that we need to set manually when running with Sparrow.
SPARK_JAVA_OPTS+=" -Dspark.broadcast.port=33624 -Dspark.driver.port=60500"
SPARK_JAVA_OPTS+=" -Dspark.io.compression.codec=spark.io.LZFCompressionCodec"
export SPARK_JAVA_OPTS
