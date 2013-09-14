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
import time

num_nodes = 100
wait_delay = 20 * 60
results_dirname = "results_per_task"
partitions = 33
reducers = 8
ratios = [(2, 2)] #-p (sample ratio unconstrianed), -q (sample ratio constrained)
rates = [310]
backend_mem = "5g"
cluster_name = "tpch"
sparrow_branch = "per_task_old_code"
#sparrow_branch = "master"
key_loc = "patkey.pem"

def run_cmd(cmd):
  subprocess.check_call(cmd, shell=True)

restart_cmd = "./ec2-exp.sh -i %s restart-spark-shark %s -m sparrow" % (key_loc, cluster_name)
start_cmd = "./ec2-exp.sh -i %s start-shark-tpch %s" % (key_loc, cluster_name)

for rate in rates:
  for (p, q) in ratios:
    dep_cmd = "./ec2-exp.sh deploy %s -g %s -i %s --reduce-tasks=%s -p %s -q %s -u %s -v %s --spark-backend-mem=%s" % (
      cluster_name, sparrow_branch, key_loc, reducers, p, q, partitions, rate, backend_mem)
    run_cmd(dep_cmd)
    run_cmd(restart_cmd)
    run_cmd(start_cmd)
    time.sleep(wait_delay)

    collect_dir = "%s" % results_dirname
    if not os.path.exists(collect_dir):
      os.mkdir(collect_dir)
    collect_cmd = "./ec2-exp.sh -i %s collect-logs %s --log-dir=%s" % (key_loc, cluster_name, collect_dir)
    run_cmd(collect_cmd)

    analysis_dir = "%s/analysis" % collect_dir
    if not os.path.exists(analysis_dir):
      os.mkdir(analysis_dir)

    run_cmd("gunzip %s/*.log.gz" % collect_dir)
    run_cmd("python /home/ubuntu/sparrow/src/main/python/parse_tpch_logs.py 600 800 " \
           "/home/ubuntu/sparrow/deploy/ec2/%s/shark_* > /home/ubuntu/sparrow/deploy/ec2/%s/tpch_results" % (collect_dir, analysis_dir))

    run_cmd("/home/ubuntu/sparrow/src/main/python/parse_logs.sh start_sec=600 " \
           "end_sec=800 log_dir=/home/ubuntu/sparrow/deploy/ec2/%s/ output_dir=/home/ubuntu/sparrow/deploy/ec2/%s/ > /home/ubuntu/sparrow/deploy/ec2/%s/parse_results" % (
               collect_dir, analysis_dir, analysis_dir))
    run_cmd("gzip %s/*.log" % collect_dir)
