#
# Copyright 2013 The Regents of The University California
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import os
import subprocess
import sys
import time

def run_cmd(cmd):
  subprocess.check_call(cmd, shell=True)


def main(argv):
  if len(argv) != 3 and len(argv) != 1:
    print "Usage: python prepare_tpch_experiments.py [opt: num_frontends num_backends]"
    print "Specifying num_frontends and num_backends will cause a cluster to be launched"
    return

  cluster_name = "tpch"
  key_loc = "patkey.pem"

  if len(argv) == 3:
      num_frontends = argv[1]
      num_backends = argv[2]

      print "Launching %s frontends and %s backends" % (num_frontends, num_backends)
# add --spot-price=foo for spot pricing
      launch_cmd = ("./ec2-exp.sh -t m2.4xlarge --spot-price=2.00 -a ami-533a733a -i %s launch %s -f %s -b %s" %
          (key_loc, cluster_name, num_frontends, num_backends))
      #launch_cmd = ("./ec2-exp.sh -t cr1.8xlarge --spot-price=2.00 -a ami-894801e0 -i %s launch %s -f %s -b %s" %
      #    (key_loc, cluster_name, num_frontends, num_backends))
      run_cmd(launch_cmd)

      print "Sleeping for 1 minute after launching machines"
      time.sleep(60)

  print "***********Attempting to stop HDFS"
  stop_hdfs_cmd = "./ec2-exp.sh -i %s stop-hdfs %s" % (key_loc, cluster_name)
  run_cmd(stop_hdfs_cmd)
 
  print "***********Deploying Code and Config Files"
  backend_mem="10g"
  deploy_cmd = "./ec2-exp.sh -i %s deploy %s --spark-backend-mem %s" % (key_loc, cluster_name, backend_mem)
  run_cmd(deploy_cmd)

  print "***********Starting HDFS"
  hdfs_cmd = "./ec2-exp.sh -i %s start-hdfs %s" % (key_loc, cluster_name)
  run_cmd(hdfs_cmd)

  scale_factor = 2
  print "***********Creating database"
  create_db_cmd = ("./ec2-exp.sh -i %s create-database %s --scale-factor %s" %
                   (key_loc, cluster_name, scale_factor))
  # This returns a non-zero exit code if some of the directorys already exist, which is
  # not an error we care about.
  subprocess.call(create_db_cmd, shell=True)

  print "***********Starting Sparrow (needed for creating denormalized Hive tables)"
  restart_cmd = "./ec2-exp.sh -i %s restart-spark-shark %s" % (key_loc, cluster_name)
  run_cmd(restart_cmd)

  print "***********Creating TPCH Tables in Shark"
  create_tables_cmd = "./ec2-exp.sh -i %s create-tpch-tables %s " % (key_loc, cluster_name)
  run_cmd(create_tables_cmd)

if __name__ == "__main__":
  main(sys.argv)
