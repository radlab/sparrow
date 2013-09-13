#!/bin/bash
#Filters out some known issues
host=`ec2metadata  |grep public-hostname | cut -d " " -f 2`
echo $host
cat /disk1/sparrow/* |egrep "ERROR|Exception" | grep -v ConfigNodeMonitorState 
