#!/bin/bash
host=`ec2metadata  |grep public-hostname | cut -d " " -f 2`
echo $host
cat /disk1/sparrow/* |egrep "ERROR|Exception" | wc  
