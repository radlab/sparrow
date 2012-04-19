#!/bin/bash
# Deploy built version of Sparrow on frontends and backends.

FRONTENDS=`cat frontends.txt`
BACKENDS=`cat backends.txt`
SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=5"

for fe in $FRONTENDS; do
  rsync -e "ssh $SSH_OPTS" --delete -az ~/ `dig +short $fe`:~/ &
done
wait

for fe in $FRONTENDS; do
  ssh $SSH_OPTS `dig +short $fe` "/root/configure_node.sh" &
done

for be in $BACKENDS; do
  rsync -e "ssh $SSH_OPTS" --delete -az ~/ `dig +short $be`:~/ &
done
wait

for be in $BACKENDS; do
  ssh $SSH_OPTS `dig +short $be` "/root/configure_node.sh" &
done
wait

if [ ! -d "/disk1/hdfs/name/current/" ]; then
  echo "Formatting Namenode"
  echo "Y" | sudo -u hdfs /opt/hadoop/bin/hadoop namenode -format
else
  echo "Namenode formatted"
fi
